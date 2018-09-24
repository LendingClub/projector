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
package org.lendingclub.mercator.docker;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.Image;
import com.github.dockerjava.api.model.Info;
import com.google.common.base.CaseFormat;
import com.google.common.base.Converter;

public class DockerSerializerModule extends SimpleModule {

	private static final long serialVersionUID = 1L;
	ObjectMapper vanillaObjectMapper = new ObjectMapper();

	public DockerSerializerModule() {
		addSerializer(Container.class, new ContainerSerializer());

		addSerializer(InspectContainerResponse.class, new InspectContainerResponseSerializer());
	}

	void renameAttribute(ObjectNode x, String key, String key2) {
		JsonNode val = x.get(key);
		if (val != null) {
			x.remove(key);
			x.set(key2, val);
		}
	}

	public ObjectNode flatten(JsonNode n) {
		ObjectNode out = vanillaObjectMapper.createObjectNode();
		Converter<String, String> caseFormat = CaseFormat.UPPER_CAMEL.converterTo(CaseFormat.LOWER_CAMEL);

		n.fields().forEachRemaining(it -> {
			JsonNode val = it.getValue();
			String key = it.getKey();
			key = caseFormat.convert(key);
			if (val.isValueNode()) {

				out.set(key, it.getValue());
			} else if (val.isArray()) {
				if (val.size() == 0) {
					out.set(key, val);
				}
				boolean valid = true;
				Class<? extends Object> type = null;
				ArrayNode an = (ArrayNode) val;
				for (int i = 0; valid && i < an.size(); i++) {
					if (!an.get(i).isValueNode()) {
						valid = false;
					}
					if (type != null && an.get(i).getClass() != type) {
						valid = false;
					}
				}

			}
		});
		renameAttribute(out, "oSType", "osType");
		renameAttribute(out, "iD", "id");
		renameAttribute(out, "neventsListener", "nEventsListener");
		renameAttribute(out, "cPUSet", "cpuSet");
		renameAttribute(out, "cPUShares", "cpuShares");
		renameAttribute(out, "iPv4Forwarding", "ipv4Forwarding");
		renameAttribute(out, "oOMKilled", "oomKilled");
		renameAttribute(out, "state_oomkilled", "state_oomKilled");
		renameAttribute(out, "bridgeNfIptables", "bridgeNfIpTables");
		renameAttribute(out, "bridgeNfIp6tables", "bridgeNfIp6Tables");
		out.remove("ngoroutines");
		return out;
	}

	class InspectContainerResponseSerializer extends StdSerializer<InspectContainerResponse> {

		private static final long serialVersionUID = 1L;

		protected InspectContainerResponseSerializer() {
			super(InspectContainerResponse.class);
		}

		@Override
		public void serialize(InspectContainerResponse value, JsonGenerator gen, SerializerProvider provider)
				throws IOException {
			ObjectNode n = (ObjectNode) vanillaObjectMapper.convertValue(value, JsonNode.class);

			ObjectNode out = flatten(n);

			addWithPrefix(out, "config", flatten(n.path("Config")));
			addWithPrefix(out, "state", flatten(n.path("State")));
			gen.writeTree(out);
		}
	}

	class ContainerSerializer extends StdSerializer<Container> {

		private static final long serialVersionUID = 1L;

		protected ContainerSerializer() {
			super(Container.class);
			// TODO Auto-generated constructor stub
		}

		@Override
		public void serialize(Container value, JsonGenerator gen, SerializerProvider provider) throws IOException {
			ObjectNode target = vanillaObjectMapper.createObjectNode();
			ObjectNode auto = (ObjectNode) vanillaObjectMapper.valueToTree(value);
			Converter<String, String> caseFormat = CaseFormat.UPPER_CAMEL.converterTo(CaseFormat.LOWER_CAMEL);

			auto.fields().forEachRemaining(it -> {

				String field = caseFormat.convert(it.getKey());
				if (it.getValue().isContainerNode()) {
					if (it.getValue().isArray()) {

						if (field.equals("names")) {
							target.set(field, it.getValue());
						}

					}
				}

				else {
					if (field.equals("imageID")) {
						field = "imageId";
					}
					target.set(field, it.getValue());
				}

			});

			gen.writeTree(target);

		}

	}

	protected void addWithPrefix(ObjectNode top, String prefix, JsonNode val) {
		val.fields().forEachRemaining(it -> {
			top.set(prefix + "_" + it.getKey(), it.getValue());
		});
	}
}
