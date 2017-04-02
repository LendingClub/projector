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
package org.lendingclub.mercator.aws;

import java.util.List;
import java.util.function.Predicate;

import org.lendingclub.neorx.NeoRxClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class ShadowAttributeRemover {

	Logger logger = LoggerFactory.getLogger(ShadowAttributeRemover.class);

	NeoRxClient neo4j;

	public ShadowAttributeRemover(NeoRxClient client) {
		this.neo4j = client;
	}

	class SanitizationFilter implements Predicate<String> {

		boolean isValidChar(char c) {
			if (c=='`' || c==';') {
					return false;
			}return true;
	}

	@Override
	public boolean test(String t) {
		for (char c : t.toCharArray()) {
			if (!isValidChar(c)) {
				return false;
			}
		}
		return true;
	}

	}

	public void removeTagAttributes(String label, JsonNode desired, JsonNode cache) {
		removeAttributes(label, desired, cache, new Predicate<String>() {

			@Override
			public boolean test(String t) {
				return t.startsWith("aws_tag_");
			}

		});
	}

	public void removeAttributes(String label, JsonNode desired, JsonNode cache, Predicate<String> predicate) {
		List<String> attrs = getAttributesToRemove(desired, cache, predicate);

		if (!attrs.isEmpty()) {
			List<String> fragments = Lists.newArrayList();
			attrs.stream().filter(new SanitizationFilter()).forEach(n -> {
				fragments.add("x.`" + n + "`");
			});
			if (!fragments.isEmpty()) {
				String clause = Joiner.on(", ").join(fragments);
				String cypher = "match (x:" + label + " {aws_arn:{aws_arn}}) remove " + clause + " return x";
				neo4j.execCypher(cypher, "aws_arn", desired.get("aws_arn").asText());
			}
		}

	}

	protected List<String> getAttributesToRemove(JsonNode actual, JsonNode neo4j, Predicate<String> predicate) {
		List<String> list = Lists.newArrayList();
		neo4j.fieldNames().forEachRemaining(n -> {
			if (n != null) {

				if (predicate.test(n)) {
					if (!actual.has(n)) {
						list.add(n);
					}

				}
			}
		});

		return list;
	}
}
