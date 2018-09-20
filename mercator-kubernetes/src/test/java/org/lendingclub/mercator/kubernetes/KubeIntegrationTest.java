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

import java.util.Optional;

import org.junit.Assume;
import org.junit.Before;
import org.lendingclub.mercator.core.Projector;
import org.lendingclub.mercator.test.MercatorIntegrationTest;
import org.lendingclub.neorx.NeoRxClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;

public abstract class KubeIntegrationTest extends MercatorIntegrationTest {

	Logger logger = LoggerFactory.getLogger(KubeIntegrationTest.class);

	private static Boolean available = null;
	private static KubeScanner scanner;

	String getTestContextName() {
		String name = System.getenv("KUBE_TEST_CONTEXT_NAME");
		if (Strings.isNullOrEmpty(name)) {
			name = "docker-for-desktop";
		}
		return name;
	}

	void resetTestScanner() {
		available=null;
		scanner=null;
	}
	public NeoRxClient getNeo4j() {
		return getTestKubeScanner().getNeoRxClient();
	}
	public KubeScanner getTestKubeScanner() {
		if (!isTestKubeClusterAvailable()) {
			throw new IllegalStateException("test kube cluster not available");
		}
		if (scanner != null) {
			return scanner;
		}
		return scanner;
	}

	public boolean isTestKubeClusterAvailable() {
		if (!isNeo4jAvailable()) {
			return false;
		}
		if (scanner != null) {
			return true;
		}
		if (available != null && available.booleanValue() == false) {
			logger.warn("*** Kubernetes not available ***");
			return false;
		}
		try {
			KubeScanner ks = getProjector().createBuilder(KubeScannerBuilder.class).withKubeConfig(getTestContextName())
					.build();
			ks.getKubernetesClient().namespaces().list().getItems();

			scanner = ks;
			available = true;
			return true;
		} catch (Exception e) {
			logger.warn("could not connect",e);
			available = false;
		}

		return available;
	}

	@Before
	public void checkLocalKubernetesAvailable() {
		Assume.assumeTrue(isTestKubeClusterAvailable());
	}

}
