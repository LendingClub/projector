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

import java.io.File;
import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.lendingclub.mercator.core.MercatorException;
import org.lendingclub.mercator.core.ScannerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Suppliers;

import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;


public class KubeScannerBuilder extends ScannerBuilder<KubeScanner> {

	Logger logger = LoggerFactory.getLogger(KubeScannerBuilder.class);
	ConfigBuilder configBuilder = new ConfigBuilder();

	Supplier<KubernetesClient> clientSupplier;
	String clusterId;
	String clusterName;

	String alternateClusterIdAttributeName;


	
	static ObjectMapper mapper = new ObjectMapper(new YAMLFactory());


	public KubeScannerBuilder withKubernetesClient(Supplier<KubernetesClient> clientSupplier) {
		this.clientSupplier = Suppliers.memoize(clientSupplier::get);
		return this;
	}

	public KubeScannerBuilder withAlternateClusterIdAttributeName(String attr) {
		this.alternateClusterIdAttributeName = attr;
		return this;
	}

	public KubeScannerBuilder withKubernetesClient(KubernetesClient client) {
		this.clientSupplier = new Supplier<KubernetesClient>() {

			@Override
			public KubernetesClient get() {
				return client;
			}
		};
		return this;
	}

	public KubeScannerBuilder withClusterName(String name) {
		this.clusterName = name;
		return this;
	}

	JsonNode getNamedSection(JsonNode top, String type, String name) {
		JsonNode list = top.path(type);
		for (int i=0; i<list.size(); i++ ) {
			JsonNode n = list.get(i);
	
			if (n.path("name").asText().equals(name)) {
				return n;
			}
		}
		return MissingNode.getInstance();
		
	}
	
	
	

	public KubeScannerBuilder withKubeConfig() {
		File f = new File(System.getProperty("user.home"),".kube/config");
		return withKubeConfig(f,null);
	}
	public KubeScannerBuilder withKubeConfig(String context) {
		File f = new File(System.getProperty("user.home"),".kube/config");
		return withKubeConfig(f,context);
	}
	public KubeScannerBuilder withKubeConfig(File f, String context) {

		try {
			ObjectNode n = (ObjectNode) mapper.readTree(f);
			
			if (Strings.isNullOrEmpty(context)) {
				context = n.path("current-context").asText();
			}

			final String fContext = context;
			JsonNode x = getNamedSection(n, "contexts", context).path("context");
			
			String user = x.path("user").asText();
			String cluster = x.path("cluster").asText();
			logger.info("context={} user={} cluster={}",context,user,cluster);
			
			JsonNode userNode = getNamedSection(n,"users",user).path("user");
			JsonNode clusterNode = getNamedSection(n,"clusters",cluster).path("cluster");
	
			
			String url = clusterNode.path("server").asText();
			boolean insecureSkipTlsVerify = clusterNode.path("insecure-skip-tls-verify").asBoolean();
			
			if (Strings.isNullOrEmpty(cluster)) {
				throw new MercatorException("could not resolve context '"+context+"' in "+f);
			}
			withClusterName(cluster);
			
			configBuilder.withMasterUrl(url).withTrustCerts(insecureSkipTlsVerify);
			
			if (userNode.has("client-certificate-data")) {
			
				configBuilder.withClientCertData(userNode.path("client-certificate-data").asText());
			}
			if (userNode.has("client-key-data")) {
			
				configBuilder.withClientKeyData(userNode.path("client-key-data").asText());
			}
			
			
			return this;
		} catch (IOException e) {
			throw new MercatorException(e);
		}
	}

	public KubeScannerBuilder withClusterId(String id) {
		this.clusterId = id;
		return this;
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

	private KubernetesClient buildClient() {
		return new DefaultKubernetesClient(configBuilder.build());
	}
	@Override
	public KubeScanner build() {

		if (clientSupplier == null) {
			this.clientSupplier = Suppliers.memoize(this::buildClient);		
		}
		KubeScanner scanner = new KubeScanner(this);
		if (Strings.isNullOrEmpty(clusterId)) {
			Preconditions.checkNotNull(scanner.getNeoRxClient());
			
		}
		Preconditions.checkState(!Strings.isNullOrEmpty(clusterName), "clusterName must be set");
		Preconditions.checkState(!Strings.isNullOrEmpty(clusterId), "clusterId must be set");

	
		scanner.clusterName = clusterName;
		scanner.clusterId = clusterId;
		scanner.alternateClusterIdAttributeName = this.alternateClusterIdAttributeName;

		return scanner;
	}

}
