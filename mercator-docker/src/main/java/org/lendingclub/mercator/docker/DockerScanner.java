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

import javax.ws.rs.client.WebTarget;

import org.lendingclub.mercator.core.AbstractScanner;
import org.lendingclub.mercator.core.Scanner;
import org.lendingclub.mercator.core.ScannerBuilder;
import org.lendingclub.mercator.core.SchemaManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.dockerjava.api.DockerClient;
import com.google.common.base.Supplier;

public class DockerScanner extends AbstractScanner {

	Logger logger = LoggerFactory.getLogger(DockerScanner.class);

	DockerScannerBuilder dockerScannerBuilder;

	static ObjectMapper mapper = new ObjectMapper();

	static {
		mapper.registerModule(new DockerSerializerModule());
	}

	public DockerScanner(ScannerBuilder<? extends Scanner> builder) {
		super(builder);

		this.dockerScannerBuilder = (DockerScannerBuilder) builder;


	}




	public DockerClient getDockerClient() {
		return dockerScannerBuilder.getDockerClient();
	}
	

	public void scanTask(String task) {
		
		try {
			SwarmScanner ss = new SwarmScanner(this);
			ss.scanTask(task);
		}
		catch (RuntimeException e) {
			maybeThrow(e);
		}
	}
	public void scanNode(String id) {
		try {
			SwarmScanner ss = new SwarmScanner(this);
			ss.scanNode(id);
		}
		catch (RuntimeException e) {
			maybeThrow(e);
		}
	}
	public void scanService(String service) {
		try {
			SwarmScanner ss = new SwarmScanner(this);
			ss.scanService(service);
		}
		catch (RuntimeException e) {
			maybeThrow(e);
		}
	}
	public void scan() {
		
		
		WebTarget wt = SwarmScanner.extractWebTarget(getDockerClient());
		JsonNode n = wt.path("/info").request().buildGet().invoke(JsonNode.class);
		
		if (n.path("Swarm").path("Cluster").isContainerNode()) {
			logger.info("talking to a docker swarm manager: {}",wt);
			SwarmScanner ss = new SwarmScanner(this);
			ss.scan();
		}
		else {
			// looks like we are talking to a docker engine.  We don't support this yet because we need to figure out
			// how to ensure that we do not get duplicate entries from the Swarm view of the nodes and the engine's view of itself.
			logger.info("talking to a docker engine: {}",wt);
			
			String engineId = n.get("ID").asText();
			JsonLogger.logInfo("x", n); 
			updateEngineInfo(n);
		
		}
		

	}
	
	private void updateEngineInfo(JsonNode nodeInfo) {
		ObjectNode n = new ObjectMapper().createObjectNode();
		n.set("engineId", nodeInfo.get("ID"));
		n.put("architecture", nodeInfo.path("Architecture").asText());
		n.put("osType", nodeInfo.path("OSType").asText());
		n.put("operatingSystem", nodeInfo.path("OperatingSystem").asText());
		n.put("kernelVersion", nodeInfo.path("KernelVersion").asText());
		n.put("serverVersion", nodeInfo.path("ServerVersion").asText());
		n.put("nCPU", nodeInfo.path("NCPU").asLong());
		n.put("memTotal", nodeInfo.path("MemTotal").asLong());
		
		n.path("Labels").fields().forEachRemaining(it -> {
			String labelKey = "label_" + it.getKey();
			n.set(labelKey, it.getValue());
		});
		
		getNeoRxClient().execCypher("merge (h:DockerHost {engineId:{engineId}}) set h+={props}, h.updateTs=timestamp()","engineId",n.path("engineId").asText(),"props",n);
		
	} 
	@Override
	public SchemaManager getSchemaManager() {
		return new DockerSchemaManager(getProjector().getNeoRxClient());
	}

	public DockerRestClient getRestClient() {
		return DockerRestClient.forDockerClient(getDockerClient());
	}
}
