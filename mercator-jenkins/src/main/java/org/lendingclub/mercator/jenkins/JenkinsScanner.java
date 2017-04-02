/**
 * Copyright 2017 Lending Club, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lendingclub.mercator.jenkins;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jdom2.Document;
import org.jdom2.Text;
import org.jdom2.filter.Filters;
import org.jdom2.xpath.XPathExpression;
import org.jdom2.xpath.XPathFactory;
import org.lendingclub.mercator.core.AbstractScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

public class JenkinsScanner extends AbstractScanner {

	Logger logger = LoggerFactory.getLogger(JenkinsScanner.class);

	Supplier<JenkinsClient> clientSupplier = Suppliers.memoize(new JenkinsClientSupplier());
	JsonNode masterNode;

	public JenkinsScanner(JenkinsScannerBuilder builder, Map<String, String> props) {
		super(builder, props);
	}

	public void scan() {

		this.masterNode = updateMasterNode();

		Iterator<JsonNode> t = getJenkinsClient().getServerInfo().path("jobs").elements();

		while (t.hasNext()) {
			try {
				JsonNode apiJob = t.next();

				scanJob(apiJob);

				String name = apiJob.path("name").asText();
				logger.info("scanning job: {}", name);
			} catch (Exception e) {
				logger.info("", e);
				;
			}
		}

	}

	public JsonNode updateMasterNode() {
		Preconditions.checkState(getProjector().getNeoRxClient() != null,
				"neorx client must be set");

		String url = getJenkinsClient().getServerUrl();

		String cypher = "merge (c:CIServer:JenkinsServer {url: {url}}) "
				+ "ON CREATE SET c.type='jenkins', "
				+ "c.createTs=timestamp(), c.updateTs=timestamp(),c.url={url} "
				+ "ON MATCH set c.updateTs=timestamp(),c.url={url} return c";

		JsonNode n = getProjector().getNeoRxClient()
				.execCypher(cypher, "url", url).blockingFirst();
		return n;
	}

	protected void scanJob(JsonNode n) {

		String cypher = "match (c:JenkinsServer {url: {serverUrl}}) MERGE (c)-[r:CONTAINS]->(j:CIJob:JenkinsJob {name: {jobName}}) "
				+ "ON CREATE  SET j.createTs=timestamp(),r.createTs=timestamp(),j.updateTs=timestamp(),r.updateTs=timestamp(),j.url={url} "
				+ "ON MATCH   SET j.url={url}, j.updateTs=timestamp(),r.updateTs=timestamp() return j,ID(j) as id";

		JsonNode x = getProjector().getNeoRxClient()
				.execCypher(cypher, "serverUrl", masterNode.get("url").asText(),
						"jobName", n.path("name").asText(), "url",
						n.path("url").asText())
			.blockingFirst();

		try {
			Document d = getJenkinsClient().getJobConfig(n.path("name").asText()).getDocument();
			Optional<String> gitUrl = extractText(d, "//hudson.plugins.git.UserRemoteConfig/url/text()");
			if (gitUrl.isPresent()) {
				cypher = "match (j:JenkinsJob {url:{jobUrl}}), (r:GitHubRepo {sshUrl:{sshUrl}}) MERGE (j)-[x:BUILDS]->(r)";
				getProjector().getNeoRxClient().execCypher(cypher, "jobUrl", n.path("url").asText(), "sshUrl", gitUrl.get());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	class JenkinsClientSupplier implements Supplier<JenkinsClient> {

		@Override
		public JenkinsClient get() {

			return new JenkinsClientImpl(getConfig().get("jenkins.url"), getConfig().get("jenkins.username"),
					getConfig().get("jenkins.password"));
		}

	}

	public JenkinsClient getJenkinsClient() {
		return clientSupplier.get();
	}

	public Optional<String> extractText(Document d, String xpath) {
		XPathExpression<Text> expression = XPathFactory.instance().compile(
				xpath, Filters.text());
		return getText(expression.evaluate(d));
	}

	Optional<String> getText(List<Text> list) {
		if (list == null || list.isEmpty()) {
			return Optional.absent();
		}
		Text text = list.get(0);
		if (text == null) {
			return Optional.absent();
		}
		return Optional.fromNullable(text.getText());
	}
}
