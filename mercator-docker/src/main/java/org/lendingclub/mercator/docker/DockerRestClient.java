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
package org.lendingclub.mercator.docker;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

import org.lendingclub.mercator.core.MercatorException;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.DockerCmdExecFactory;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.jaxrs.JerseyDockerCmdExecFactory;

/**
 * The docker java client is really not that useful for much other than
 * connection management. It is out-of-date and all the bean mapping just gets
 * in the way. This exposes the raw API via Jackson TreeModel. Awesome!
 * 
 * @author rschoening
 *
 */
public class DockerRestClient {

	private WebTarget webTarget;

	public static DockerRestClient forDockerClient(DockerClient client) {
		DockerRestClient x = new DockerRestClient();
		x.webTarget = extractWebTarget(client);
		return x;
	}

	public WebTarget getWebTarget() {
		return webTarget;
	}

	public JsonNode get(String... paths) {

		try {
			WebTarget t = webTarget;
			for (String p : paths) {
				t = t.path(p);
			}
			JsonNode n = t.request().accept("application/json").get(JsonNode.class);
			return n;
		} catch (RuntimeException e) {
			Throwable t = e.getCause();
			if (t != null && t instanceof NotFoundException) {
				throw new org.lendingclub.mercator.core.NotFoundException(t);
			}

			throw new MercatorException(e);
		}

	}

	public JsonNode getInfo() {
		return get("/info");
	}

	public JsonNode getNodes() {
		return get("/nodes");
	}

	public JsonNode getNode(String id) {
		return get("/nodes", id);
	}

	public JsonNode getTask(String task) {
		return get("/tasks", task);
	}

	public JsonNode getTasks() {
		return get("/tasks");
	}

	public JsonNode getContainers() {
		return get("/containers");
	}

	public JsonNode getContainer(String c) {
		return get("/containers", c);
	}

	public JsonNode getServices() {
		return get("/services");
	}

	public JsonNode getService(String c) {
		return get("/services", c);
	}

	public JsonNode getSwarm() {
		return get("/swarm");
	}

	public Optional<String> getSwarmClusterId() {
		return Optional.ofNullable(getInfo().path("Swarm").path("Cluster").path("ID").asText(null));
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
}
