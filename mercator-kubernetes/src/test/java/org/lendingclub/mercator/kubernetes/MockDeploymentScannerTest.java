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
package org.lendingclub.mercator.kubernetes;

import java.util.List;
import java.util.UUID;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.lendingclub.mercator.core.JsonUtil;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;

public class MockDeploymentScannerTest extends MockKubeClusterTest {
	@Test
	public void testNamespaces() {
		createStandardTestData();
		
		scanner.scanNamespaces();
		
		List<JsonNode> results =
				neo4j.execCypher("match (c:KubeCluster {clusterName:{clusterName}})--(n:KubeNamespace) return c,n",
						"clusterName", clusterName).toList().blockingGet();
		
		results.forEach(it->{
			JsonUtil.logInfo("", it);
		});
		Assertions.assertThat(results.stream().allMatch(n->n.path("c").path("clusterId").asText().equals(clusterId))).isTrue();
		Assertions.assertThat(results.stream()).allMatch(n->Math.abs(System.currentTimeMillis()-n.path("c").path("updateTs").asLong())<5000);
		Assertions.assertThat(results.stream())
		.allMatch(n->Math.abs(System.currentTimeMillis()-n.path("n").path("updateTs").asLong())<5000);
		
		results.forEach(it->{
			Assertions.assertThat(it.path("n").path("clusterId").asText()).isEqualTo(clusterId);
			Assertions.assertThat(it.path("n").path("annotations")).contains(TextNode.valueOf("aname"));
			Assertions.assertThat(it.path("n").path("labels")).contains(TextNode.valueOf("lname"));
			Assertions.assertThat(it.path("n").path("annotation_aname").asText()).contains(it.path("n").path("name").asText());
			Assertions.assertThat(it.path("n").path("label_lname").asText()).contains(it.path("n").path("name").asText());
		});
	}
	
	@Test
	public void testDeployments() {
		scanner.scanDeployments();
		List<JsonNode> results =
				neo4j.execCypher("match (c:KubeCluster {clusterName:{clusterName}})--(n:KubeNamespace)--(d:KubeDeployment) return d",
						"clusterName", clusterName).toList().blockingGet();
		
		results.forEach(deployment->{
			JsonUtil.logInfo("", deployment);
		});
		
	
		Assertions.assertThat(results.stream().filter(p->p.path("name").asText().equals("d1")).count()).isEqualTo(2);
		Assertions.assertThat(results.stream().filter(p->p.path("name").asText().equals("d1")))
				.allMatch(p->p.path("name").asText().equals("d1"))
				.allMatch(p->p.path("clusterId").asText().equals(clusterId))
				.allMatch(p->p.path("annotation_ns").asText().equals(p.path("namespace").asText()));
		
		Assertions.assertThat(results.stream().filter(p->p.path("namespace").asText().equals("ns1") && p.path("name").asText().equals("d1"))).isNotEmpty();

		client.extensions().deployments().inNamespace("ns1").withName("d1").delete();
		
		scanner.scan();
		
		client.extensions().deployments().inAnyNamespace().list().getItems().forEach(it->{
			JsonUtil.logInfo("deployment", it);
		});
		
		results =
				neo4j.execCypher("match (c:KubeCluster {clusterName:{clusterName}})--(n:KubeNamespace)--(d:KubeDeployment) return d",
						"clusterName", clusterName).toList().blockingGet();
		
		Assertions.assertThat(results.stream().filter(p->p.path("namespace").asText().equals("ns1") && p.path("name").asText().equals("d1"))).isEmpty();
		
		
		// Note: the mock server does not seem to support in-place edits, so the following does not add the label fizz=buzz
		//client.extensions().deployments().inNamespace("ns2").withName("d1").edit().editMetadata().addToLabels("fizz", "buzz").endMetadata().done();
		
		
		
	
	}
	
	@Test
	public void testDeleteAndRecreateDeployment() {
		// Delete d1 in ns2 and replace it with another of the same name, but different uid
				String uid = UUID.randomUUID().toString();
				client.extensions().deployments().inNamespace("ns2").withName("d1").delete();
				client.extensions().deployments().inNamespace("ns2").withName("d1").createNew()
				.withNewMetadata()
				.withUid(uid)
				.withName("d1")
				.withNamespace("ns2")
				.endMetadata()
				.done();
				
				scanner.scanDeployments("ns2","d1");
				List<JsonNode> results =
						neo4j.execCypher("match (c:KubeCluster {clusterName:{clusterName}})--(n:KubeNamespace)--(d:KubeDeployment) return d",
								"clusterName", clusterName).toList().blockingGet();
				
				Assertions.assertThat(results.stream().filter(p->p.path("namespace").asText().equals("ns2") && p.path("name").asText().equals("d1")))
				.hasSize(1)
				.allMatch(p->p.path("uid").asText().equals(uid));
	}
	
	@Test
	public void testDeleteAndRecreateDeploymentWithFullScan() {
		// Delete d1 in ns2 and replace it with another of the same name, but different uid
				String uid = UUID.randomUUID().toString();
				client.extensions().deployments().inNamespace("ns2").withName("d1").delete();
				client.extensions().deployments().inNamespace("ns2").withName("d1").createNew()
				.withNewMetadata()
				.withUid(uid)
				.withName("d1")
				.withNamespace("ns2")
				.endMetadata()
				.done();
				
				scanner.scanDeployments();
				List<JsonNode> results =
						neo4j.execCypher("match (c:KubeCluster {clusterName:{clusterName}})--(n:KubeNamespace)--(d:KubeDeployment) return d",
								"clusterName", clusterName).toList().blockingGet();
				
				Assertions.assertThat(results.stream().filter(p->p.path("namespace").asText().equals("ns2") && p.path("name").asText().equals("d1")))
				.hasSize(1)
				.allMatch(p->p.path("uid").asText().equals(uid));
	}
}
