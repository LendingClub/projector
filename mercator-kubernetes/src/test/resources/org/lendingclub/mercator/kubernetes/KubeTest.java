package org.lendingclub.mercator.kubernetes;

import java.io.File;

import org.junit.Test;
import org.lendingclub.mercator.test.MercatorIntegrationTest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;

public class KubeTest extends MercatorIntegrationTest {

	@Test
	public void testIt() {

		try {
			KubeScanner scanner = getProjector().createBuilder(KubeScannerBuilder.class).withDefaultKubeConfig()
					.withCurrentContext().build();

			scanner.scan();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
