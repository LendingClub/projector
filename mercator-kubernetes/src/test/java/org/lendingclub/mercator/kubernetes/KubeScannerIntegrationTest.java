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

import java.sql.Time;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.assertj.core.api.Assertions;
import org.assertj.core.data.Offset;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;

public class KubeScannerIntegrationTest extends KubeIntegrationTest {

	Logger logger = LoggerFactory.getLogger(KubeScannerIntegrationTest.class);
	static ObjectMapper mapper = new ObjectMapper();

	@Test
	public void testBasics() {
		Assertions.assertThat(getTestKubeScanner()).isSameAs(getTestKubeScanner());
		Assertions.assertThat(getTestKubeScanner().getKubernetesClient())
				.isSameAs(getTestKubeScanner().getKubernetesClient());
		Assertions.assertThat(getTestKubeScanner().getClusterName()).isNotNull();
		Assertions.assertThat(getTestKubeScanner().getClusterId()).isNotNull();
	}

	private void nukeAll() {
		nukeAllNodesWithClusterId(getTestKubeScanner().getClusterId());
	}

	private void nukeAllNodesWithClusterId(String id) {
		getTestKubeScanner().getNeoRxClient().execCypher(
				"match (a) where labels(a)[0]=~'Kube.*' and labels(a)[0]<>'KubeCluster' and a.clusterId={clusterId} detach delete a", "clusterId", id);
		resetTestScanner();

	}


	void log(String msg, Object val) {
		try {
			logger.info("{} - \n{}",msg, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(val));
		} catch (Exception e) {
			logger.error("problem logging", e);
		}
	}

	@Test
	public void testEndpoints() {
		nukeAll();
		getTestKubeScanner().scan();

		getTestKubeScanner().getNeoRxClient().execCypher("match (a:KubePod) detach delete a");
		getTestKubeScanner().scanServices();

	}
	
	@Test
	public void testXX() {
		getTestKubeScanner().scanReplicaSets();
	}
	@Test
	public void testIt() {
		nukeAll();
		getTestKubeScanner().scan();

		JsonNode n = getNeo4j().execCypher("match (a:KubeCluster {clusterId:{clusterId}}) return a", "clusterId",
				getTestKubeScanner().getClusterId()).blockingSingle();
		Assertions.assertThat(n.path("clusterName").asText()).isEqualTo(getTestKubeScanner().getClusterName());
		Assertions.assertThat(n.path("clusterId").asText()).isEqualTo(getTestKubeScanner().getClusterId());
		Assertions.assertThat(n.path("createTs").asLong()).isCloseTo(System.currentTimeMillis(),
				Offset.offset(TimeUnit.SECONDS.toMillis(10)));
		Assertions.assertThat(n.path("updateTs").asLong()).isCloseTo(System.currentTimeMillis(),
				Offset.offset(TimeUnit.SECONDS.toMillis(10)));
		Assertions.assertThat(n.path("url").asText()).startsWith("http");

	}
	
	@Test
	public void testNamespaces() {
		getTestKubeScanner().scan();
		List<JsonNode> namespaces = getNeo4j()
				.execCypher("match (a:KubeCluster {clusterId:{clusterId}})--(n:KubeNamespace) return n", "clusterId",
						getTestKubeScanner().getClusterId())
				.toList().blockingGet();

		Assertions
				.assertThat(
						namespaces.stream().map(x -> x.path("name").asText()).anyMatch(x -> x.equals("kube-system")))
				.isTrue();
		Assertions.assertThat(namespaces.stream().map(x -> x.path("name").asText()).anyMatch(x -> x.equals("default")))
				.isTrue();
		namespaces.forEach(it -> {
			
			Assertions.assertThat(it.has("uid")).isTrue();
			Assertions.assertThat(it.has("name")).isTrue();
			Assertions.assertThat(it.has("clusterId")).isTrue();
			Assertions.assertThat(it.path("selfLink").asText())
					.contains("/api/v1/namespaces/" + it.path("name").asText());
			Assertions.assertThat(it.path("updateTs").asLong()).isCloseTo(System.currentTimeMillis(),
					Offset.offset(TimeUnit.SECONDS.toMillis(10)));
		});
	}
	@Test
	public void testReplicaSets() {
		getTestKubeScanner().scanReplicaSets();
	}
	@Test
	public void testNodes() {
		getTestKubeScanner().scan();
		List<JsonNode> nodes = getNeo4j()
				.execCypher("match (a:KubeCluster {clusterId:{clusterId}})--(n:KubeNode) return n", "clusterId",
						getTestKubeScanner().getClusterId())
				.toList().blockingGet();

		Assertions.assertThat(nodes.size()).isGreaterThanOrEqualTo(1);
		
		AtomicReference<String> nameRef = new AtomicReference<String>(null);
		nodes.forEach(it -> {
			log("KubeNode",it);
			nameRef.set(it.path("name").asText());
			Assertions.assertThat(it.has("uid")).isTrue();
			Assertions.assertThat(it.has("name")).isTrue();
			Assertions.assertThat(it.has("clusterId")).isTrue();
			
			Assertions.assertThat(it.path("selfLink").asText())
					.contains("/api/v1/nodes/");
			
			Assertions.assertThat(it.path("updateTs").asLong()).isCloseTo(System.currentTimeMillis(),
					Offset.offset(TimeUnit.SECONDS.toMillis(10)));
		});
		String uuid = UUID.randomUUID().toString();
		getTestKubeScanner().getKubernetesClient().nodes().withName(nameRef.get()).edit().editMetadata().addToLabels("junit", uuid).endMetadata().done();
		
		getTestKubeScanner().scanNodes();
		
		nodes = getNeo4j()
				.execCypher("match (a:KubeCluster {clusterId:{clusterId}})--(n:KubeNode {name:{name}}) return n", "clusterId",
						getTestKubeScanner().getClusterId(),"name",nameRef.get())
				.toList().blockingGet();
		nodes.forEach(it -> {
			log("KubeNode",it);
			Assertions.assertThat(it.path("label_junit").asText()).isEqualTo(uuid);
		});
		
		getTestKubeScanner().getKubernetesClient().nodes().withName(nameRef.get()).edit().editMetadata().removeFromLabels("junit").endMetadata().done();
		getTestKubeScanner().scanNodes();
		
		nodes = getNeo4j()
				.execCypher("match (a:KubeCluster {clusterId:{clusterId}})--(n:KubeNode {name:{name}}) return n", "clusterId",
						getTestKubeScanner().getClusterId(),"name",nameRef.get())
				.toList().blockingGet();
		nodes.forEach(it -> {
			log("KubeNode",it);
			Assertions.assertThat(it.has("label_junit")).isFalse();
		});
	}
	

}
