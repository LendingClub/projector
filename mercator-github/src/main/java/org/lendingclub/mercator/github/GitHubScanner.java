/**
 * Copyright 2017-2018 LendingClub, Inc.
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
package org.lendingclub.mercator.github;

import java.io.IOException;

import org.kohsuke.github.GHOrganization;
import org.kohsuke.github.GHRepository;
import org.kohsuke.github.GitHub;
import org.lendingclub.mercator.core.AbstractScanner;
import org.lendingclub.mercator.core.MercatorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

public class GitHubScanner extends AbstractScanner {
	private ObjectMapper mapper = new ObjectMapper();

	Logger logger = LoggerFactory.getLogger(GitHubScanner.class);
	Supplier<GitHub> supplier;

	
	public GitHubScanner(GitHubScannerBuilder builder) {
		super(builder);
		supplier = Suppliers.memoize(new GitHubSupplier());
	}

	@Override
	public void scan() {
		try {
			getGitHubClient().getMyOrganizations().forEach((name, org) -> {
				scanOrganization(org);
			});
		} catch (IOException e) {
			throw new MercatorException(e);
		}

	}

	public void scanOrganization(String name) {
		try {
			logger.info("scanning org: {}",name);
			GHOrganization org = getGitHubClient().getOrganization(name);
	
			scanOrganization(org);
		} catch (IOException e) {
			throw new MercatorException(e);
		}
	}

	public void scanOrganization(GHOrganization org) {

		try {
			ObjectNode n = mapper.createObjectNode();
			n.put("url", org.getUrl().toString());
			if (org.getHtmlUrl() != null) {
				n.put("htmlUrl", org.getHtmlUrl().toString());
			}
			n.put("name", org.getLogin());
			n.put("displayName", org.getName());
		
			String cypher = "merge (o:GitHubOrg {url:{url}}) set o+={props},o.updateTs=timestamp() return o";
			getProjector().getNeoRxClient().execCypher(cypher, "url", n.path("url").asText(), "props", n);

		} catch (IOException e) {
			throw new MercatorException(e);
		}
		try {
			org.getRepositories().values().forEach(repo -> {

				scanRepository(repo);

				String cypher = "match (o:GitHubOrg {url:{orgUrl}}), (r:GitHubRepo {url:{repoUrl}}) merge (o)-[x:CONTAINS]->(r) set x.updateTs=timestamp()";

				getProjector().getNeoRxClient().execCypher(cypher, "orgUrl", org.getUrl().toString(), "repoUrl",
						repo.getUrl().toString());
			});
		} catch (IOException e) {
			throw new MercatorException(e);
		}
	}

	public void scanRepository(String name) {
		try {
			scanRepository(getGitHubClient().getRepository(name));
		} catch (IOException e) {
			throw new MercatorException(e);
		}

	}

	public void scanRepository(GHRepository repo) {

		logger.info("scanning repo: {}", repo.getName());
		ObjectNode n = mapper.createObjectNode();

		n.put("name", repo.getName());
		n.put("description", repo.getDescription());
		n.put("fullName", repo.getFullName());
		n.put("homepage", repo.getHomepage());
		n.put("sshUrl", repo.getSshUrl());
		if (repo.getHtmlUrl() != null) {
			n.put("htmlUrl", repo.getHtmlUrl().toString());
		}
		n.put("url", repo.getUrl().toString());
		n.put("gitTransportUrl", repo.getGitTransportUrl());

		String cypher = "merge (r:SCMRepo:GitHubRepo {url:{url}}) set r+={props},r.updateTs=timestamp() return r";

		getProjector().getNeoRxClient().execCypher(cypher, "url", repo.getUrl().toString(), "props", n);

	}

	class GitHubSupplier implements Supplier<GitHub> {
		public GitHub get() {
			try {
				GitHubScannerBuilder ghb = (GitHubScannerBuilder) getBuilder();
				String url = ghb.url;
				String token = ghb.token ;
				String username = ghb.username;
				String password = ghb.password;

				if (!Strings.isNullOrEmpty(url)) {
					// enterprise
					if (!Strings.isNullOrEmpty(token)) {
						return GitHub.connectToEnterprise(url, token);
					}
					if (!Strings.isNullOrEmpty(username)) {
						return GitHub.connectToEnterprise(url, username, password);
					}

					return GitHub.connectToEnterpriseAnonymously(url);

				} else {
					if (!Strings.isNullOrEmpty(token)) {
						return GitHub.connectUsingOAuth(token);
					}
					if (!Strings.isNullOrEmpty(username)) {
						return GitHub.connectUsingPassword(username, password);
					}

					return GitHub.connectAnonymously();
				}
			} catch (IOException e) {
				throw new MercatorException(e);
			}

		}
	}

	public GitHub getGitHubClient() {
		return supplier.get();
	}
}
