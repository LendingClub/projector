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
package org.lendingclub.mercator.kubernetes;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.lendingclub.mercator.core.MercatorException;
import org.lendingclub.mercator.core.ScannerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Preconditions;
import com.google.common.hash.Hashing;

import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.zjsonpatch.internal.guava.Strings;

public class KubeScannerBuilder extends ScannerBuilder<KubeScanner> {

	Logger logger = LoggerFactory.getLogger(KubeScannerBuilder.class);
	ConfigBuilder configBuilder = new ConfigBuilder();
	KubernetesClient kubernetesClient;
	JsonNode kubeConfig;
	String clusterId;
	String clusterName;
	
	public KubeScannerBuilder withKubernetesClient(KubernetesClient client) {
		this.kubernetesClient = client;
		return this;
	}

	public KubeScannerBuilder withClusterName(String name) {
		this.clusterName = name;
		return this;
	}
	
	private void setClusterIdIfNotSet(String n) {
		if (Strings.isNullOrEmpty(clusterId)) {
			this.clusterId = n;
		}
	}
	public KubeScannerBuilder withKubeConfigCluster(String cluster) {
		kubeConfig.path("clusters").forEach(it->{
			if (it.path("name").asText().equals(cluster)) {
				withClusterName(it.path("name").asText());
				setClusterIdIfNotSet(Hashing.sha1().hashBytes(it.path("name").asText().getBytes()).toString());
				String server = it.path("cluster").path("server").asText();
				withConfig(c->{
					c.withMasterUrl(server);
				});
			}
		});
		return this;
	}
	public KubeScannerBuilder withKubeConfigUser(String user) {
		
		kubeConfig.path("users").forEach(it->{
			if (it.path("name").asText().equals(user)) {
				String authToken = it.path("user").path("auth-provider").path("config").path("access-token").asText();
				withConfig(c->{
					logger.info("setting token");
					c.withOauthToken(authToken);
				});
			}
		});
		return this;
	}
	public KubeScannerBuilder withDefaultKubeConfig() {
		return withKubeConfig(new File(System.getProperty("user.home"), ".kube/config"));
	}
	
	public KubeScannerBuilder withCurrentContext() {
		return withContext("current-context");
	}
	public KubeScannerBuilder withClusterId(String id) {
		this.clusterId = id;
		return this;
	}
	public KubeScannerBuilder withContext(String name) {
		Preconditions.checkNotNull(kubeConfig);

	
	
		AtomicBoolean found = new AtomicBoolean(false);
		kubeConfig.path("contexts").forEach(it->{
			String contextName = it.path("name").asText();
		
			if (it.path("name").asText().equals(contextName)) {
				found.set(true);
			
				withKubeConfigUser(it.path("context").path("user").asText());
				withKubeConfigCluster(it.path("context").path("cluster").asText());
			}
		});		
		if (found.get()==false) {
			throw new MercatorException("cotnext not found: "+name);
		}
		return this;
	}
	
	
	public KubeScannerBuilder withKubeConfig(File f) {
		try {
			ObjectMapper m = new ObjectMapper(new YAMLFactory());
			kubeConfig = m.readTree(f);
			return this;
		} catch (IOException e) {
			throw new MercatorException(e);
		}
	}

	public KubeScannerBuilder withUrl(String url) {
		return withConfig(cfg -> {
			cfg.withMasterUrl(url);
		});
	}

	public KubeScannerBuilder withConfig(Consumer<ConfigBuilder> consumer) {
		consumer.accept(configBuilder);
		return this;
	}

	@Override
	public KubeScanner build() {

			if (kubernetesClient == null) {
			kubernetesClient = new DefaultKubernetesClient(configBuilder.build());
		}
			Preconditions.checkState(!Strings.isNullOrEmpty(clusterName),"clusterName must be set");
			Preconditions.checkState(!Strings.isNullOrEmpty(clusterId),"clusterId must be set");

		KubeScanner scanner =  new KubeScanner(this);
		scanner.clusterName = clusterName;
		scanner.clusterId = clusterId;
		return scanner;
	}

}
