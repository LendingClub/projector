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
package org.lendingclub.mercator.core;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import io.reactivex.functions.Consumer;

public class GraphDocGenerator {

	static Logger logger = LoggerFactory.getLogger(GraphDocGenerator.class);
	static ObjectMapper yaml = new ObjectMapper(new YAMLFactory());
	Projector projector;
	File dataModelDir;

	Predicate<String> includeLabels;
	BiFunction<String, String, Boolean> attributePredicate;

	public class NodeMetadataConsumer implements Consumer<JsonNode> {

		File dir;

		public NodeMetadataConsumer(File dir) {
			this.dir = dir;
		}

		@Override
		public void accept(JsonNode t) throws Exception {

			String label = t.path("label").asText();

			if (includeLabels != null && !includeLabels.test(label)) {
				return;
			}
			try {
				ObjectNode y = (ObjectNode) load(label);

				map(y, label, t.path("node"));
				String s = yaml.writerWithDefaultPrettyPrinter().writeValueAsString(y);

			} catch (Exception e) {
			}

		}

		ObjectNode getAttributeNode(ArrayNode attributes, String name) {

			Iterator<JsonNode> t = attributes.elements();
			while (t.hasNext()) {
				JsonNode attr = t.next();
				if (attr.path("name").asText().equals(name)) {
					return ObjectNode.class.cast(attr);
				}

			}
			ObjectNode attrNode = yaml.createObjectNode();

			attrNode.put("name", name);
			attrNode.put("type", "string");
			attrNode.put("description", "");
			attrNode.put("example", "");
			attributes.add(attrNode);
			return attrNode;
		}

		void map(ObjectNode yaml, String label, JsonNode n) throws IOException {
			yaml.put("type", label);
			if (!yaml.has("description")) {
				yaml.put("description", "");
			}
			ArrayNode attributes = yaml.withArray("attributes");

			n.fieldNames().forEachRemaining(attr -> {

				if (attributePredicate == null || attributePredicate.apply(label, attr)) {
					ObjectNode attrNode = getAttributeNode(attributes, attr);

				}

			});
			File f = new File(dir, label + ".yaml");
			logger.info("writing {}", f.getAbsolutePath());
			GraphDocGenerator.yaml.writerWithDefaultPrettyPrinter().writeValue(f, yaml);

		}

		JsonNode load(String label) throws IOException {
			File f = new File(dir, label + ".yaml");
			if (f.exists()) {
				return yaml.readTree(f);
			}
			return yaml.createObjectNode();
		}
	}

	public GraphDocGenerator(Projector projector) {
		this.projector = projector;
	}

	public <T extends GraphDocGenerator> T withDataModelDir(File f) {
		this.dataModelDir = f;
		return (T) this;
	}

	public GraphDocGenerator renderMarkDown() {
		File[] files = this.dataModelDir.listFiles();
		if (files == null) {
			return this;
		}
		for (File f : files) {
			try {
				if (f.isFile() && f.getName().endsWith(".yaml")) {
					renderMarkDown(f);
				}
			} catch (IOException e) {
				logger.warn("", e);
			}
		}
		return this;
	}

	public GraphDocGenerator renderMarkDown(File f) throws IOException {
		JsonNode n = yaml.readTree(f);

		File markDownFile = new File(f.getParentFile(), f.getName().replace(".yaml", ".md"));
		PrintWriter pw = new PrintWriter(new FileWriter(markDownFile));
		pw.println("## " + n.path("type").asText());
		pw.println();
		pw.println("### Attributes");
		pw.println("|Name|Type|Description|Example|");
		pw.println("|------|-------|-------|------|");
		n.path("attributes").forEach(attr -> {
			pw.println("| " + attr.path("name").asText() + "|" + attr.path("type").asText() + " |"
					+ attr.path("description").asText() + "|" + attr.path("example").asText() + "|");
		});
		pw.println();

		pw.println("### Relationships");
		pw.println();
		if (n.path("relationships").size() > 0) {
		
			pw.println("|Name|Target|Direction|");
			pw.println("|------|------|------|");
			n.path("relationships").forEach(attr -> {
				pw.println("| " + attr.path("name").asText() + "|" + attr.path("target").asText() + " |"
						+ attr.path("direction").asText() + "|");
			});
		}
		else {
			pw.println("None");
		}
		pw.println();
		pw.close();
		System.out.println(yaml.writerWithDefaultPrettyPrinter().writeValueAsString(n));
		return this;
	}

	public GraphDocGenerator scanAll(int limit) {
		projector.getNeoRxClient().execCypher("match (a) return distinct labels(a)[0] as label").forEach(it -> {

			if (includeLabels == null || includeLabels.test(it.asText())) {
				logger.info("scanning label: " + it.asText());
				scan(it.asText(), 100);
			}
		});
		return this;
	}

	public <T extends GraphDocGenerator> T withMercatorSourceDocs() {
		return withDataModelDir(new File("./docs/graph-doc"));
	}

	public <T extends GraphDocGenerator> T withLabelPredicate(Predicate<String> p) {
		this.includeLabels = p;
		return (T) this;
	}

	public <T extends GraphDocGenerator> T withAttributePredicate(BiFunction<String, String, Boolean> p) {
		this.attributePredicate = p;
		return (T) this;
	}

	public void scan(String type, int limit) throws JsonProcessingException {

		NodeMetadataConsumer c = new NodeMetadataConsumer(dataModelDir);
		projector.getNeoRxClient()
				.execCypher("match (a:" + type + ") return '" + type + "' as label, a as node limit " + limit)
				.forEach(c);

	}

}
