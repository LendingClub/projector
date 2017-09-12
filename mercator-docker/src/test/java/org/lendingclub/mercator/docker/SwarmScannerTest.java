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
package org.lendingclub.mercator.docker;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.lendingclub.mercator.core.Projector;
import org.lendingclub.mercator.test.MercatorIntegrationTest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class SwarmScannerTest extends MercatorIntegrationTest {

	ObjectMapper mapper = new ObjectMapper();

	@Test
	public void testFlattenTask() throws Exception {
		String taskJson = " {\n" + "  \"ID\" : \"kz0xbugh9xnf4bqcxjqc97lo3\",\n" + "  \"Version\" : {\n"
				+ "    \"Index\" : 112\n" + "  },\n" + "  \"CreatedAt\" : \"2017-09-08T21:24:42.408242959Z\",\n"
				+ "  \"UpdatedAt\" : \"2017-09-09T02:12:26.443758461Z\",\n" + "  \"Labels\" : { },\n"
				+ "  \"Spec\" : {\n" + "    \"ContainerSpec\" : {\n"
				+ "      \"Image\" : \"trident-echo:latest@sha256:8c850603e5b67909da2409003454d91a154b5dacb2d9add580355f8a42ea2ae0\",\n"
				+ "      \"Env\" : [ \"path=/api/foo\", \"app=foo\" ],\n" + "      \"DNSConfig\" : { }\n" + "    },\n"
				+ "    \"Resources\" : {\n" + "      \"Limits\" : { },\n" + "      \"Reservations\" : { }\n"
				+ "    },\n" + "    \"Placement\" : {\n" + "      \"Platforms\" : [ {\n"
				+ "        \"Architecture\" : \"amd64\",\n" + "        \"OS\" : \"linux\"\n" + "      } ]\n"
				+ "    },\n" + "    \"ForceUpdate\" : 0\n" + "  },\n"
				+ "  \"ServiceID\" : \"sx6r9h4zsyosjqgfx2x66zv7a\",\n" + "  \"Slot\" : 2,\n"
				+ "  \"NodeID\" : \"un7e89zvlnb4c2l11yvmsvlio\",\n" + "  \"Status\" : {\n"
				+ "    \"Timestamp\" : \"2017-09-09T02:12:26.350558639Z\",\n" + "    \"State\" : \"shutdown\",\n"
				+ "    \"Message\" : \"shutdown\",\n" + "    \"ContainerStatus\" : {\n"
				+ "      \"ContainerID\" : \"1812bf07dea79f54c22313d4fe3de263abfc0bfebee9014b09a748208c00aa39\",\n"
				+ "      \"ExitCode\" : 143\n" + "    },\n" + "    \"PortStatus\" : { }\n" + "  },\n"
				+ "  \"DesiredState\" : \"shutdown\"\n" + "}";
		ObjectNode n = (ObjectNode) mapper.readTree(taskJson);
		SwarmScanner scanner = new SwarmScanner(null);
		scanner.swarmClusterId = "abcd123";
		JsonNode flattened = scanner.flattenTask(n);

		JsonLogger.logInfo("foo", flattened);
		Assertions.assertThat(flattened.get("swarmClusterId").asText()).isEqualTo("abcd123");
		Assertions.assertThat(flattened.get("taskId").asText()).isEqualTo("kz0xbugh9xnf4bqcxjqc97lo3");
		Assertions.assertThat(flattened.get("serviceId").asText()).isEqualTo("sx6r9h4zsyosjqgfx2x66zv7a");
		Assertions.assertThat(flattened.get("swarmNodeId").asText()).isEqualTo("un7e89zvlnb4c2l11yvmsvlio");
		Assertions.assertThat(flattened.get("state").asText()).isEqualTo("shutdown");
		Assertions.assertThat(flattened.get("message").asText()).isEqualTo("shutdown");
		Assertions.assertThat(flattened.get("desiredState").asText()).isEqualTo("shutdown");
		Assertions.assertThat(flattened.get("containerId").asText())
				.isEqualTo("1812bf07dea79f54c22313d4fe3de263abfc0bfebee9014b09a748208c00aa39");

	}

	@Test
	public void testLabels() {
		// we don't want to use LocalDockerDaemonIntegrationTest because it will only work with a local docker daemon
		Projector p = getProjector();
		SwarmScanner ss = new SwarmScanner(p.createBuilder(DockerScannerBuilder.class).build());
		
		String id = "junit-"+System.currentTimeMillis();
		JsonNode actual = p.getNeoRxClient().execCypher("merge (x:DockerService {serviceId:{id}}) set x.label_c='c', x.label_foo='foo', x.junitData=true return x","id",id).blockingFirst();
		
		JsonNode intended = mapper.createObjectNode().put("label_a", "1").put("label_b", "2").put("chinacat","sunflower");
		
		p.getNeoRxClient().execCypher("merge (a:DockerService {serviceId:{id}}) set a+={props} return a","id",id,"props",intended);
	
		ss.removeDockerLabels("DockerService","serviceId",id,intended, actual);
		
		JsonNode result = p.getNeoRxClient().execCypher("match (x:DockerService {serviceId:{id}}) return x","id",id).blockingFirst();
		Assertions.assertThat(result.path("serviceId").asText()).isEqualTo(id);
		Assertions.assertThat(result.has("label_c")).isFalse();
		Assertions.assertThat(result.has("label_foo")).isFalse();
		Assertions.assertThat(result.get("label_a").asText()).isEqualTo("1");
	}
}
