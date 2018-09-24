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
import org.assertj.core.api.Condition;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.lendingclub.mercator.core.JsonUtil;
import org.lendingclub.mercator.test.MercatorIntegrationTest;
import org.lendingclub.neorx.NeoRxClient;
import org.mockito.Mockito;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableMap;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import io.sundr.codegen.model.JavaTypeFluent.GenericTypesNested;

public abstract class MockKubeClusterTest extends MercatorIntegrationTest {

	@Rule
	public KubernetesServer server = new KubernetesServer(true, true);

	KubeScanner scanner;

	String clusterId;
	String clusterName;

	KubernetesClient client;
	NeoRxClient neo4j;

	public void addNamespace(String name) {
		Namespace ns = server.getClient().namespaces().withName(name).get();
		if (ns == null) {
			server.getClient().namespaces().withName(name).createNew().withNewMetadata().withName(name)
			.addToLabels("lname", name)
			.addToAnnotations("aname","my name is "+name)
					.withUid(UUID.randomUUID().toString()).endMetadata().done();
		}

	}

	@Before
	public void setup() {
		if (isNeo4jAvailable()) {
			getProjector().getNeoRxClient()
					.execCypher("match (a:KubeCluster)--(x) where a.clusterName=~'junit.*' detach delete a,x");
			getProjector().getNeoRxClient()
					.execCypher("match (a:KubeCluster) where a.clusterName=~'junit.*' detach delete a");

			clusterId = "kc-" + UUID.randomUUID().toString();
			clusterName = "junit-" + System.currentTimeMillis();
			scanner = getProjector().createBuilder(KubeScannerBuilder.class).withKubernetesClient(server.getClient())
					.withClusterId(clusterId).withClusterName(clusterName).build();
			client = server.getClient();
			neo4j = getProjector().getNeoRxClient();
			addNamespace("default");
			createStandardTestData();
			scanner.scan();
		}
	}

	protected void createStandardTestData() {
		addNamespace("default");
		addNamespace("ns1");
		addNamespace("ns2");
		
		client.extensions().deployments().inNamespace("ns1").createNew().withNewMetadata()
		.withUid(UUID.randomUUID().toString())
		.withName("d1")
		.withNamespace("ns1")
		.addToAnnotations("ns", "ns1")
		.endMetadata().withKind("Deployment").withNewSpec().endSpec().done();
		
		client.extensions().deployments().inNamespace("ns2").createNew().withNewMetadata()
		.withUid(UUID.randomUUID().toString())
		.withName("d1")
		.withNamespace("ns2")
		.addToAnnotations("ns", "ns2")
		.endMetadata().withNewSpec().endSpec().done();
		
	
	}
	
	@Test
	public void testShouldDelete() {
		

		Assertions.assertThat(KubeScanner.shouldDelete(null, "foo")).isTrue();
		
		HasMetadata md = Mockito.mock(HasMetadata.class);
		
		Assertions.assertThat(KubeScanner.shouldDelete(md, null)).isFalse();
		Assertions.assertThat(KubeScanner.shouldDelete(md, "")).isFalse();
		
		String uid = UUID.randomUUID().toString();
		ObjectMeta omd = new ObjectMeta();
		omd.setUid(uid);
		Mockito.when(md.getMetadata()).thenReturn(omd);
	
		
		Assertions.assertThat(KubeScanner.shouldDelete(md, UUID.randomUUID().toString())).isTrue();
		Assertions.assertThat(KubeScanner.shouldDelete(md, uid)).isFalse();
	}

}
