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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.ws.rs.client.WebTarget;

import org.lendingclub.mercator.core.NotFoundException;
import org.lendingclub.neorx.NeoRxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.DockerCmdExecFactory;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.jaxrs.JerseyDockerCmdExecFactory;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.Cache;
import com.google.common.collect.Lists;

public class SwarmScanner {

	ObjectMapper mapper = new ObjectMapper();
	Logger logger = LoggerFactory.getLogger(SwarmScanner.class);
	DockerScanner dockerScanner;
	String swarmClusterId = null;

	SwarmScanner(DockerScanner ds) {
		this.dockerScanner = ds;
	}

	public DockerRestClient getRestClient() {
		return DockerRestClient.forDockerClient(dockerScanner.getDockerClient());
	}

	long saveDockerNode(String swarmClusterId, JsonNode n) {

		String swarmNodeId = n.get("swarmNodeId").asText();
		AtomicLong updateTs = new AtomicLong(Long.MAX_VALUE);
		dockerScanner.getNeoRxClient()
				.execCypher(
						"merge (n:DockerHost {swarmNodeId:{nodeId}}) set n+={props}, n.updateTs=timestamp() return n",
						"nodeId", swarmNodeId, "props", n)
				.forEach(actual -> {
					removeDockerLabels("DockerHost", "swarmNodeId", swarmNodeId, n, actual);
					updateTs.set(Math.min(updateTs.get(), actual.path("updateTs").asLong(Long.MAX_VALUE)));
				});

		logger.info("connecting swarm={} to node={}", swarmClusterId, swarmNodeId);
		dockerScanner.getNeoRxClient().execCypher(
				"match (s:DockerSwarm {swarmClusterId:{swarmClusterId}}), (n:DockerHost {swarmNodeId:{nodeId}}) merge (s)-[x:CONTAINS]->(n) set x.updateTs=timestamp()",
				"swarmClusterId", swarmClusterId, "nodeId", swarmNodeId);
		return updateTs.get();

	}

	JsonNode flattenSwarmNode(JsonNode n) {
		ObjectNode out = mapper.createObjectNode();
		out.put("swarmNodeId", n.path("ID").asText());
		out.put("swarmClusterId", getSwarmClusterId().get());
		out.put("role", n.path("Spec").path("Role").asText());
		out.put("availability", n.path("Spec").path("Availability").asText());
		out.put("hostname", n.path("Description").path("Hostname").asText());
		out.put("engineVersion", n.path("Description").path("Engine").path("EngineVersion").asText());
		out.put("state", n.path("Status").path("State").asText());
		out.put("addr", n.path("Status").path("Addr").asText());
		out.put("leader", n.path("ManagerStatus").path("Leader").asBoolean());

		return out;
	}

	/**
	 * The Docker java client is significantly behind the server API. Rather than
	 * try to fork/patch our way to success, we just implement a bit of magic to get
	 * access to the underlying jax-rs WebTarget.
	 * 
	 * Docker should just expose this as a public method.
	 * 
	 * @param c
	 * @return
	 */
	public static WebTarget extractWebTarget(DockerClient c) {

		try {
			for (Field m : DockerClientImpl.class.getDeclaredFields()) {

				if (DockerCmdExecFactory.class.isAssignableFrom(m.getType())) {
					m.setAccessible(true);
					JerseyDockerCmdExecFactory f = (JerseyDockerCmdExecFactory) m.get(c);
					Method method = f.getClass().getDeclaredMethod("getBaseResource");
					method.setAccessible(true);
					return (WebTarget) method.invoke(f);
				}
			}
		} catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
			throw new IllegalStateException("could not obtain WebTarget", e);
		}
		throw new IllegalStateException("could not obtain WebTarget");
	}

	protected boolean isUnixDomainScoket(String jerseyUri) {
		// Jersey reports the URL of a unix domain socket as
		// http://localhost:80.
		return (jerseyUri != null && jerseyUri.startsWith("unix://"));

	}

	public void scan() {
		WebTarget t = extractWebTarget(dockerScanner.getDockerClient());
		logger.info("Scanning {}", t);
		JsonNode response = t.path("/info").request().buildGet().invoke(JsonNode.class);

		JsonNode swarm = response.path("Swarm");
		JsonNode cluster = swarm.path("Cluster");
		String swarmClusterId = cluster.path("ID").asText();

		// need to parse these dates
		String createdAt = cluster.path("CreatedAt").asText();
		String updatedAt = cluster.path("UpdatedAt").asText();
		ObjectNode props = mapper.createObjectNode();
		props.put("swarmClusterId", swarmClusterId);
		props.put("createdAt", createdAt);
		props.put("updatedAt", updatedAt);

		JsonNode swarmNode = dockerScanner.getNeoRxClient()
				.execCypher(
						"merge (c:DockerSwarm {swarmClusterId:{id}}) set c+={props},c.updateTs=timestamp() return c",
						"id", swarmClusterId, "props", props)
				.blockingFirst(MissingNode.getInstance());

		if (isUnixDomainScoket(t.getUri().toString())) {
			// Only set managerApiUrl to a unix domain socket if it has not
			// already been set.
			// This is useful for trident
			if (!isUnixDomainScoket(swarmNode.path("managerApiUrl").asText())) {

				String LOCAL_DOCKER_DAEMON_SOCKET_URL = "unix:///var/run/docker.sock";
				logger.info("setting mangerApiUrl to {} for swarm {}", LOCAL_DOCKER_DAEMON_SOCKET_URL, swarmClusterId);

				String name = "local";
				dockerScanner.getNeoRxClient().execCypher("match (c:DockerSwarm {name:{name}}) return c", "name", name)
						.forEach(it -> {
							String oldSwarmClusterId = it.path("swarmClusterId").asText();
							if (!swarmClusterId.equals(oldSwarmClusterId)) {
								dockerScanner.getNeoRxClient().execCypher(
										"match (c:DockerSwarm {swarmClusterId:{swarmClusterId}}) detach delete c",
										"swarmClusterId", oldSwarmClusterId);
							}
						});

				dockerScanner.getNeoRxClient().execCypher(
						"match (c:DockerSwarm {swarmClusterId:{id}}) set c.managerApiUrl={managerApiUrl},c.name={name},c.tridentClusterId={name} return c",
						"id", swarmClusterId, "managerApiUrl", LOCAL_DOCKER_DAEMON_SOCKET_URL, "name", name);

			}
		}

		AtomicBoolean fail = new AtomicBoolean(false);
		response = t.path("/nodes").request().buildGet().invoke(JsonNode.class);
		AtomicLong earliestTimestamp = new AtomicLong(Long.MAX_VALUE);
		response.elements().forEachRemaining(it -> {
			try {
				earliestTimestamp
						.set(Math.min(earliestTimestamp.get(), saveDockerNode(swarmClusterId, flattenSwarmNode(it))));
			} catch (RuntimeException e) {
				logger.warn("problem", e);
				fail.set(true);
			}
		});

		if (!fail.get()) {
			if (earliestTimestamp.get() < System.currentTimeMillis()) {
				logger.info("deleting DockerHost nodes before with updateTs<{}", earliestTimestamp.get());
				dockerScanner.getNeoRxClient().execCypher(
						"match (s:DockerSwarm {swarmClusterId:{id}})--(x:DockerHost) where s.updateTs>x.updateTs detach delete x",
						"id", swarmClusterId);
			}
		}
		scanServicesForSwarm(swarmClusterId);
		scanTasksForSwarm(swarmClusterId);
	}

	protected String formatLabelKey(String key) {
		return "label_" + key;
	}

	ObjectNode flattenService(JsonNode n) {
		ObjectNode out = new ObjectMapper().createObjectNode();
		out.put("serviceId", n.path("ID").asText());
		out.put("versionIndex", n.path("Version").path("Index").asInt());
		out.put("createdAt", n.path("CreatedAt").asText());
		out.put("updatedAt", n.path("UpdatedAt").asText());
		out.put("name", n.path("Spec").path("Name").asText());

		JsonNode labels = n.path("Spec").path("Labels");

		labels.fields().forEachRemaining(it -> {
			out.put(formatLabelKey(it.getKey()), it.getValue().asText());

		});
		JsonNode taskTemplate = n.path("Spec").path("TaskTemplate");
		out.put("taskImage", taskTemplate.path("ContainerSpec").path("Image").asText());
		out.set("taskArgs", taskTemplate.path("ContainerSpec").path("Args"));
		out.put("replicas", n.path("Spec").path("Mode").path("Replicated").path("Replicas").asInt());
		return out;
	}

	protected ObjectNode flattenTask(JsonNode n) {

		ObjectNode out = new ObjectMapper().createObjectNode();
		out.put("swarmClusterId", getSwarmClusterId().get());
		out.put("taskId", n.path("ID").asText());
		out.put("versionIndex", n.path("Version").path("Index").asText());
		out.put("createdAt", n.path("CreatedAt").asText());
		out.put("updatedAt", n.path("UpdatedAt").asText());
		n.path("Labels").fields().forEachRemaining(it -> {
			out.put(formatLabelKey(it.getKey()), it.getValue().asText());
		});
		n.path("Spec").path("ContainerSepc");
		out.put("serviceId", n.path("ServiceID").asText());

		out.put("swarmNodeId", n.path("NodeID").asText());
		out.put("state", n.path("Status").path("State").asText());
		out.put("message", n.path("Status").path("Message").asText());
		out.put("containerId", n.path("Status").path("ContainerStatus").path("ContainerID").asText());
		out.put("desiredState", n.path("DesiredState").asText());

		n.path("Status").path("PortStatus").path("Ports").forEach(it -> {

			String mode = it.path("PublishMode").asText();
			String protocol = it.path("Protocol").asText();
			if (mode.equals("host") && protocol.equals("tcp")) {

				int targetPort = it.path("TargetPort").asInt();
				int publishedPort = it.path("PublishedPort").asInt();
				String key = String.format("hostTcpPortMap_%d", targetPort);
				out.put(key, publishedPort);
			}

		});
		;

		return out;
	}

	public void scanServicesForSwarm(String swarmClusterId) {

		JsonNode response = getRestClient().getServices();

		AtomicLong earlistUpdate = new AtomicLong(Long.MAX_VALUE);
		AtomicBoolean error = new AtomicBoolean(false);
		response.forEach(it -> {
			try {
				ObjectNode n = flattenService(it);
				n.put("swarmClusterId", swarmClusterId);
				dockerScanner.getNeoRxClient().execCypher(
						"merge (x:DockerService {serviceId:{serviceId}}) set x+={props}, x.updateTs=timestamp() return x",
						"serviceId", n.get("serviceId").asText(), "props", n).forEach(svc -> {
							removeDockerLabels("DockerService", "serviceId", n.get("serviceId").asText(), n, svc);
							earlistUpdate
									.set(Math.min(earlistUpdate.get(), svc.path("updateTs").asLong(Long.MAX_VALUE)));
						});
				dockerScanner.getNeoRxClient().execCypher(
						"match (swarm:DockerSwarm {swarmClusterId:{swarmClusterId}}),(service:DockerService{serviceId:{serviceId}}) merge (swarm)-[x:CONTAINS]->(service) set x.updateTs=timestamp()",
						"swarmClusterId", swarmClusterId, "serviceId", n.path("serviceId").asText());

			} catch (Exception e) {
				logger.warn("problem updating service", e);
				error.set(true);
			}
		});
		if (error.get() == false) {
			if (earlistUpdate.get() < System.currentTimeMillis()) {
				dockerScanner.getNeoRxClient().execCypher(
						"match (x:DockerService) where x.swarmClusterId={swarmClusterId} and x.updateTs<{cutoff} detach delete x",
						"cutoff", earlistUpdate.get(), "swarmClusterId", swarmClusterId);
			}
		}

	}

	public void scanNode(String nodeId) {
		try {
			JsonNode task = getRestClient().getNode(nodeId);
			saveTask(task);
		} catch (RuntimeException e) {
			if (isNotFound(e)) {
				deleteNode(nodeId);
			} else {
				throw e;
			}
		}
	}

	public void scanTask(String taskId) {
		try {
			JsonNode task = getRestClient().getTask(taskId);
			saveTask(task);
		} catch (RuntimeException e) {
			if (isNotFound(e)) {
				deleteTask(taskId);
			} else {
				throw e;
			}
		}
	}

	public void deleteTask(String id) {
		Optional<String> scid = getSwarmClusterId();
		if (scid.isPresent()) {
			dockerScanner.getNeoRxClient().execCypher(
					"match (s:DockerSwarm {swarmClusterId:{swarmClusterId}})--(x:DockerTask {taskId:{taskId}}) detach delete x",
					"taskId", id, "swarmClusterId", scid.get());
			dockerScanner.getNeoRxClient().execCypher(
					"match (x:DockerTask {taskId:{taskId}, swarmClusterId:{swarmClusterId}}) detach delete x", "taskId",
					id, "swarmClusterId", scid.get());
		}
		return;
	}

	public void deleteNode(String id) {
		Optional<String> scid = getSwarmClusterId();
		if (scid.isPresent()) {
			dockerScanner.getNeoRxClient().execCypher(
					"match (s:DockerSwarm {swarmClusterId:{swarmClusterId}})--(x:DockerHost {swarmNodeId:{swarmNodeId}}) detach delete x",
					"swarmNodeId", id, "swarmClusterId", scid.get());
			dockerScanner.getNeoRxClient().execCypher(
					"match (x:DockerHost {swarmNodeId:{swarmNodeId}, swarmClusterId:{swarmClusterId}}) detach delete x",
					"swarmNodeId", id, "swarmClusterId", scid.get());
		}
		return;
	}

	private void checkNotEmpty(String val, String msg) {
		Preconditions.checkState(!Strings.isNullOrEmpty(val), "" + msg + " must be set");
	}

	protected long saveTask(JsonNode it) {

		ObjectNode n = flattenTask(it);

	
		n.put("swarmClusterId", getSwarmClusterId().get());

		String taskId = n.get("taskId").asText();
		String serviceId = n.path("serviceId").asText();
		String swarmNodeId = n.path("swarmNodeId").asText();
		checkNotEmpty(taskId, "taskId");
		checkNotEmpty(serviceId, "serviceId");
		
		AtomicLong timestamp = new AtomicLong(Long.MAX_VALUE);
		dockerScanner.getNeoRxClient()
				.execCypher("merge (x:DockerTask {taskId:{taskId}}) set x+={props}, x.updateTs=timestamp() return x",
						"taskId", taskId, "props", n)
				.forEach(tt -> {

					timestamp.set(tt.path("updateTs").asLong(Long.MAX_VALUE));

					removeDockerLabels("DockerTask", "taskId", taskId, n, it);
				});

		{
			// it might be worth it to select these relationships and only
			// update if they are missing
			dockerScanner.getNeoRxClient().execCypher(
					"match (s:DockerService {serviceId:{serviceId}}),(t:DockerTask{taskId:{taskId}}) merge (s)-[x:CONTAINS]->(t) set x.updateTs=timestamp() return t,s",
					"serviceId", serviceId, "taskId", taskId);
			if (!Strings.isNullOrEmpty(swarmNodeId)) {
				dockerScanner.getNeoRxClient().execCypher(
						"match (h:DockerHost {swarmNodeId:{swarmNodeId}}), (t:DockerTask {swarmNodeId:{swarmNodeId}}) merge (h)-[x:RUNS]->(t) set x.updateTs=timestamp()",
						"swarmNodeId", swarmNodeId);
			}
		}
		return timestamp.get();
	}

	public void scanTasksForSwarm(String swarmClusterId) {

		logger.info("scanning tasks for swarm: {}", swarmClusterId);

		AtomicLong earlistUpdate = new AtomicLong(Long.MAX_VALUE);
		AtomicBoolean error = new AtomicBoolean(false);
		JsonNode response = getRestClient().getTasks();
		response.forEach(it -> {
			try {
				earlistUpdate.set(Math.min(earlistUpdate.get(), saveTask(it)));

			} catch (Exception e) {
				logger.warn("problem updating task", e);
				error.set(true);
			}
		});

		if (error.get() == false) {
			if (earlistUpdate.get() < System.currentTimeMillis()) {
				dockerScanner.getNeoRxClient().execCypher(
						"match (x:DockerTask) where x.swarmClusterId={swarmClusterId} and x.updateTs<{cutoff} detach delete x",
						"cutoff", earlistUpdate.get(), "swarmClusterId", swarmClusterId);
			}
		}

	}

	public Optional<String> getSwarmClusterId() {
		if (swarmClusterId != null) {
			return Optional.of(swarmClusterId);
		}
		swarmClusterId = getRestClient().getInfo().path("Swarm").path("Cluster").path("ID").asText(null);
		return Optional.ofNullable(swarmClusterId);
	}

	public void scanService(String id) {
		try {

			Optional<String> scid = getSwarmClusterId();
			if (scid.isPresent()) {
				logger.info("performing targeted scan of service={}", id);

				JsonNode it = getRestClient().getService(id);
				ObjectNode n = flattenService(it);

				n.put("swarmClusterId", scid.get());
				dockerScanner.getNeoRxClient().execCypher(
						"merge (x:DockerService {serviceId:{serviceId}}) set x+={props}, x.updateTs=timestamp() return x",
						"serviceId", n.get("serviceId").asText(), "props", n).forEach(actual -> {
							try {
								removeDockerLabels("DockerService", "serviceId", n.get("serviceId").asText(), n,
										actual);
							} catch (Exception e) {
								logger.warn("problem removing labels: " + e.toString());
							}
						});

				dockerScanner.getNeoRxClient().execCypher(
						"match (swarm:DockerSwarm {swarmClusterId:{swarmClusterId}}),(service:DockerService{serviceId:{serviceId}}) merge (swarm)-[x:CONTAINS]->(service) set x.updateTs=timestamp()",
						"swarmClusterId", scid.get(), "serviceId", n.path("serviceId").asText());
			}
		} catch (RuntimeException e) {
			if (isNotFound(e)) {
				deleteService(id);
				return;
			}
			throw e;
		}
	}

	public void deleteService(String id) {
		Optional<String> scid = getSwarmClusterId();
		if (scid.isPresent()) {
			logger.info("deleting neo4j DockerService id/name={}", id);
			dockerScanner.getNeoRxClient().execCypher(
					"match (x:DockerService {name:{name},swarmClusterId:{swarmClusterId}}) detach delete x", "name", id,
					"swarmClusterId", scid.get());
			dockerScanner.getNeoRxClient().execCypher(
					"match (x:DockerService {serviceId:{serviceId},swarmClusterId:{swarmClusterId}}) detach delete x",
					"serviceId", id, "swarmClusterId", scid.get());

			dockerScanner.getNeoRxClient().execCypher(
					"match (s:DockerSwarm {swarmClusterId:{swarmClusterId}})--(x:DockerService {serviceId:{serviceId}}) detach delete x",
					"serviceId", id, "swarmClusterId", scid.get());
			dockerScanner.getNeoRxClient().execCypher(
					"match (s:DockerSwarm {swarmClusterId:{swarmClusterId}})--(x:DockerService {name:{name}}) detach delete x",
					"name", id, "swarmClusterId", scid.get());
		}
	}

	/**
	 * Removes labels that were present from a scan, but subsequently removed.
	 * 
	 * @param neo4jLabel
	 * @param idName
	 * @param idVal
	 * @param intended
	 * @param actual
	 */
	protected void removeDockerLabels(String neo4jLabel, String idName, String idVal, JsonNode intended,
			JsonNode actual) {

		List<String> x = Lists.newArrayList();
		actual.fieldNames().forEachRemaining(it -> {
			if (it != null && it.startsWith("label_")) {
				if (!intended.has(it)) {

					if (!it.contains(" ")) {
						x.add("a.`" + it + "`");
					}
				}
			}
		});
		if (!x.isEmpty()) {
			String cypher = "match (a:" + neo4jLabel + " {" + idName + ":{id}}) remove " + Joiner.on(", ").join(x)
					+ " return a";

			dockerScanner.getNeoRxClient().execCypher(cypher, "id", idVal);
		}

	}

	protected boolean isNotFound(Throwable e) {
		if (e == null) {
			return false;
		}
		if (e instanceof NotFoundException || e instanceof com.github.dockerjava.api.exception.NotFoundException) {
			return true;
		}
		return isNotFound(e.getCause());
	}
}
