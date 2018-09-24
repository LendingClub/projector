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
package org.lendingclub.mercator.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JsonConverter {

	static ObjectMapper mapper = new ObjectMapper();

	boolean flattenNestedObjects;

	public JsonConverter() {
	}
	
	public JsonConverter withFlattenNestedObjects(boolean v) {
		this.flattenNestedObjects = v;
		return this;
	}

	public ObjectNode toJson(Object x) {
		ObjectNode n = x instanceof ObjectNode ? (ObjectNode) x : mapper.valueToTree(x);

		ObjectNode r = mapper.createObjectNode();

		flatten(r, "aws_", n);

		n.path("tags").iterator().forEachRemaining(it -> {
			String tagKey = "aws_tag_" + it.path("key").asText();
			r.put(tagKey, it.path("value").asText());
		});
	
		return r;
	}
	
	private String getKey(String key) {
		if (Character.isUpperCase(key.charAt(0))) {
			return Character.toLowerCase(key.charAt(0)) + key.substring(1);
		}
		return key;
	}

	private void flatten(ObjectNode n, String prefix, ObjectNode src) {
		src.fields().forEachRemaining(it -> {
			if (flattenNestedObjects && it.getValue().isObject()) {
				flatten(n, prefix + getKey(it.getKey()) + "_", (ObjectNode) it.getValue());
			}
			if (!it.getValue().isContainerNode()) {
				String key = getKey(it.getKey());
				n.set(prefix + key, it.getValue());
			} 
		});
	}

}
