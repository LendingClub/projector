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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.lendingclub.mercator.core.MercatorException;
import org.lendingclub.mercator.core.ScannerBuilder;
import org.neo4j.driver.v1.Config.ConfigBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DefaultDockerClientConfig.Builder;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class DockerScannerBuilder extends ScannerBuilder<DockerScanner> {

	static Logger logger = LoggerFactory.getLogger(DockerScannerBuilder.class);

	String LOCAL_DOCKER_DAEMON = "unix:///var/run/docker.sock";

	DockerClient dockerClient;
	Builder configBuilder = DefaultDockerClientConfig.createDefaultConfigBuilder();
	
	
	@Override
	public DockerScanner build() {
		if (dockerClient==null) {
			DefaultDockerClientConfig cc = configBuilder.build();
			this.dockerClient  = DockerClientBuilder.getInstance(cc).build();
		}
		return new DockerScanner(this);
	}

	
	

	public DockerScannerBuilder() {
		DefaultDockerClientConfig.createDefaultConfigBuilder();
	}

	public DockerClient getDockerClient() {
		return dockerClient;
	}
	public DockerScannerBuilder withDockerClient(DockerClient c) {
		this.dockerClient = c;
		return this;
	}
	public DockerScannerBuilder withConfig(Consumer<Builder> b) {
		b.accept(configBuilder);
		return this;
	}
	public DockerScannerBuilder withLocalDockerDaemon() {
		configBuilder.withDockerHost(LOCAL_DOCKER_DAEMON);
		return this;
	}


}
