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

import org.lendingclub.mercator.core.AbstractScanner;
import org.lendingclub.mercator.core.Scanner;
import org.lendingclub.mercator.core.ScannerBuilder;
import org.lendingclub.mercator.core.ScannerContext;
import org.lendingclub.mercator.core.ScannerContext.Invokable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.hash.Hashing;

import io.fabric8.kubernetes.api.model.ContainerState;
import io.fabric8.kubernetes.api.model.ContainerStateRunning;
import io.fabric8.kubernetes.api.model.ContainerStateTerminated;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeSpec;
import io.fabric8.kubernetes.api.model.NodeStatus;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.zjsonpatch.internal.guava.Strings;

public class KubeScanner extends AbstractScanner {

	static ObjectMapper mapper = new ObjectMapper();
	String clusterId;
	String clusterName;
	KubernetesClient client;

	public KubeScanner(ScannerBuilder<? extends Scanner> builder) {
		super(builder);
		this.clusterId = KubeScannerBuilder.class.cast(builder).clusterId;
		client = KubeScannerBuilder.class.cast(builder).kubernetesClient;
		clusterName = KubeScannerBuilder.class.cast(builder).clusterName;

	}

	public String getClusterName() {
		Preconditions.checkState(!Strings.isNullOrEmpty(clusterName), "clusterName must be set");
		return clusterName;
	}

	public String getClusterId() {
		Preconditions.checkState(!Strings.isNullOrEmpty(clusterId), "clusterId must be set");
		return clusterId;
	}

	public KubernetesClient getKubernetesClient() {
		return client;
	}

	public void scanCluster() {

		ObjectNode props = mapper.createObjectNode();
		if (!Strings.isNullOrEmpty(clusterName)) {
			props.put("clusterName", clusterName);
		}
		getNeoRxClient().execCypher("merge (a:KubeCluster {clusterId:{clusterId}})  "
				+ "on create set a+={props},a.url={url}, a.updateTs=timestamp(),a.createTs=timestamp() on match set a+={props},a.url={url}, a.updateTs=timestamp()",
				"clusterId", getClusterId(), "url", client.getMasterUrl().toString(), "props", props);

	}

	public void scanNamespace(Namespace ns) {

		ObjectNode n = mapper.createObjectNode();
		ObjectMeta md = ns.getMetadata();

		n.put("uid", ns.getMetadata().getUid());
		n.put("name", ns.getMetadata().getName());
		n.put("selfLink", md.getSelfLink());
		n.put("clusterId", getClusterId());

		getNeoRxClient().execCypher("merge (a:KubeNamespace {uid:{uid}}) set a+={props}, a.updateTs=timestamp()", "uid",
				md.getUid(), "props", n);
	}

	public void scanNamespaces() {
		getKubernetesClient().namespaces().list().getItems().forEach(it -> {
			scanNamespace(it);
		});
		getNeoRxClient().execCypher(
				"match (n:KubeNamespace {clusterId:{clusterId}}), (c:KubeCluster {clusterId:{clusterId}}) merge (c)-[x:CONTAINS]->(n)",
				"clusterId", getClusterId());
	}

	public void scanPod(Pod pod) throws JsonProcessingException {

		long ts = getCurrentTimestamp();
		ObjectNode n = mapper.createObjectNode();

		ObjectMeta meta = pod.getMetadata();
		n.put("uid", meta.getUid());
		n.put("resourceVersion", meta.getResourceVersion());
		n.put("name", meta.getName());
		n.put("namespace", meta.getNamespace());
		n.put("clusterName", meta.getClusterName());

		n.put("generateName", meta.getGenerateName());
		n.put("creationTimestamp", meta.getCreationTimestamp());
		n.put("deletionTimestamp", meta.getDeletionTimestamp());
		n.put("deletionGracePeriod", meta.getDeletionGracePeriodSeconds());
		n.put("selfLink", meta.getSelfLink());

		PodSpec spec = pod.getSpec();

		n.put("restartPolicy", spec.getRestartPolicy());
		n.put("hostNetwork", spec.getHostNetwork());
		n.put("activeDeadlineSeconds", spec.getActiveDeadlineSeconds());
		n.put("automountServiceAccountToken", spec.getAutomountServiceAccountToken());
		n.put("dnsPolicy", spec.getDnsPolicy());
		n.put("hostIPC", spec.getHostIPC());
		n.put("hostname", spec.getHostname());
		n.put("hostPID", spec.getHostPID());
		n.put("nodeName", spec.getNodeName());
		n.put("schedulerName", spec.getSchedulerName());
		n.put("serviceAccount", spec.getServiceAccount());
		n.put("serviceAccountName", spec.getServiceAccountName());
		n.put("subdomain", spec.getSubdomain());
		n.put("terminationGracePeriodSeconds", spec.getTerminationGracePeriodSeconds());

		PodStatus ps = pod.getStatus();

		n.put("hostIp", ps.getHostIP());
		n.put("message", ps.getMessage());
		n.put("phase", ps.getPhase());
		n.put("podIp", ps.getPodIP());
		n.put("qosClass", ps.getQosClass());
		n.put("reason", ps.getReason());
		n.put("startTime", ps.getStartTime());

		n.put("clusterId", getClusterId());
		getNeoRxClient().execCypher(
				"merge (p:KubePod {uid:{uid}}) on create set p+={props},p.createTs=timestamp(), p.updateTs=timestamp() on match set p+={props},p.updateTs=timestamp()",
				"uid", meta.getUid(), "props", n);

		ps.getContainerStatuses().forEach(it -> {
			ObjectNode cs = mapper.createObjectNode();
			cs.put("podUid", meta.getUid());
			cs.put("containerId", it.getContainerID());
			cs.put("image", it.getImage());
			cs.put("imageId", it.getImageID());
			cs.put("name", it.getName());
			cs.put("ready", it.getReady());
			cs.put("restartCount", it.getRestartCount());

			ContainerState state = it.getLastState();
			ContainerStateRunning running = state.getRunning();
			if (running != null) {

				cs.put("startedAt", state.getRunning().getStartedAt());
			}
			ContainerStateTerminated t = state.getTerminated();
			if (t != null) {
				cs.put("finishedAt", t.getFinishedAt());
				cs.put("exitCode", t.getExitCode());
				cs.put("message", t.getMessage());
				cs.put("reason", t.getReason());
				cs.put("signal", t.getSignal());
			}

			cs.put("clusterId", getClusterId());
			getNeoRxClient().execCypher(
					"merge (x:KubeContainer {containerId:{containerId}}) on match set x+={props},x.updateTs=timestamp() on create set x+={props}, x.updateTs=timestamp(), x.createTs=timestamp()",
					"containerId", it.getContainerID(), "props", cs);
		});

		spec.getContainers().forEach(it -> {
			ObjectNode cspec = mapper.createObjectNode();
			ArrayNode args = mapper.createArrayNode();
			it.getArgs().forEach(arg -> {
				args.add(arg);
			});
			cspec.set("args", args);
			cspec.put("workingDir", it.getWorkingDir());
			cspec.put("tty", it.getTty());
			cspec.put("imagePullPolicy", it.getImagePullPolicy());
			cspec.put("image", it.getImage());
			cspec.put("terminationMessagePath", it.getTerminationMessagePath());
			cspec.put("terminationMessagePolicy", it.getTerminationMessagePolicy());
			cspec.put("stdin", it.getStdin());
			cspec.put("stdinOnce", it.getStdinOnce());

			it.getEnv().forEach(env -> {
				cspec.put("env_" + env.getName(), env.getValue());
			});
			getNeoRxClient().execCypher(
					"merge (k:KubeContainerSpec {podUid:{podUid}, name:{name}})"
							+ " on create set k+={props}, k.updateTs=timestamp(),k.createTs=timestamp() "
							+ " on match  set k+={props}, k.updateTs=timestamp()",
					"podUid", meta.getUid(), "name", it.getName(), "props", cspec);

			getNeoRxClient().execCypher(
					"match (c:KubeContainer {podUid:{uid},name:{name}}), (cs:KubeContainerSpec {podUid:{uid},name:{name}}) MERGE (c)-[x:USES]->(cs)",
					"uid", meta.getUid(), "name", it.getName());

		});

		// merge container spec to pod
		getNeoRxClient().execCypher(
				"match (p:KubePod {uid:{uid}}), (c:KubeContainerSpec {podUid:{uid}}) MERGE (p)-[x:CONTAINS]->(c) set x.updateTs=timestamp()",
				"uid", meta.getUid());

		// merge pod to container
		getNeoRxClient().execCypher(
				"match (p:KubePod {uid:{uid}}), (c:KubeContainer {podUid:{uid}}) MERGE (p)-[x:CONTAINS]->(c) set x.updateTs=timestamp()",
				"uid", meta.getUid());

		getNeoRxClient().execCypher(
				"match (a:KubePod {uid:{uid}})--(c:KubeContainer) where c.updateTs<{ts} detach delete c", "uid",
				meta.getUid(), "ts", ts);
	}

	long getCurrentTimestamp() {
		return getNeoRxClient().execCypher("return timestamp()").blockingFirst().asLong();
	}

	public void scanNode(Node node) {
		ObjectNode n = mapper.createObjectNode();
		ObjectMeta meta = node.getMetadata();
		n.put("nodeUid", meta.getUid());
		n.put("resourceVersion", meta.getResourceVersion());
		n.put("name", meta.getName());
		n.put("namespace", meta.getNamespace());
		
		
		n.put("clusterName", meta.getClusterName());

		n.put("generateName", meta.getGenerateName());
		n.put("creationTimestamp", meta.getCreationTimestamp());
		n.put("deletionTimestamp", meta.getDeletionTimestamp());
		n.put("deletionGracePeriod", meta.getDeletionGracePeriodSeconds());
		n.put("selfLink", meta.getSelfLink());

		NodeStatus ns = node.getStatus();

		NodeSpec nodeSpec = node.getSpec();

		n.put("externalId", nodeSpec.getExternalID());
		n.put("unschedulable", nodeSpec.getUnschedulable());
		n.put("podCIDR", nodeSpec.getPodCIDR());
		n.put("providerId", nodeSpec.getProviderID());

		n.put("clusterId", clusterId);
		getNeoRxClient().execCypher("merge (n:KubeNode {nodeUid:{nodeUid}}) set n.clusterId={clusterId}, n+={props}",
				"nodeUid", meta.getUid(), "props", n, "clusterId", clusterId);

	}

	public void scanNodes() {
		ScannerContext sc = new ScannerContext();

		sc.withName("KubeNode").exec(ctx -> {
			getKubernetesClient().nodes().list().getItems().forEach(it -> {
				scanNode(it);
			});
			
		});
		
	/*	getNeoRxClient().execCypher(
				"match (n:KubeNode {clusterId:{clusterId}}),(ns:KubeNamespace {clusterId:{clusterId}}) "
						+ "where n.namespace=ns.name "
						+ " merge (ns)-[c:CONTAINS]->(n) on create set c.createTs=timestamp()  set c.updateTs=timestamp()",
				"clusterId", clusterId);
				*/
		getNeoRxClient().execCypher(
				"match (n:KubeNode {clusterId:{clusterId}}),(c:KubeCluster {clusterId:{clusterId}}) "
						
						+ " merge (c)-[x:CONTAINS]->(n) on create set c.createTs=timestamp()  set c.updateTs=timestamp()",
				"clusterId", clusterId);
	}

	public void scanPods() {

		ScannerContext sc = new ScannerContext();
		long ts = getCurrentTimestamp();
		sc.withName("KubernetesPod").exec(ctx -> {

			getKubernetesClient().pods().inAnyNamespace().list().getItems().forEach(it -> {
				try {
					scanPod(it);
					ctx.incrementEntityCount();
				} catch (Exception e) {
					maybeThrow(e);
				}
			});
		});

		getNeoRxClient().execCypher(
				"match (c:KubeNamespace {clusterId:{clusterId}}), (p:KubePod {clusterId:{clusterId}})  where p.namespace=c.name merge (c)-[x:CONTAINS]->(p) set x.updateTs=timestamp()",
				"clusterId", getClusterId());

		if (!sc.hasExceptions()) {
			getNeoRxClient().execCypher(
					"match (p:KubePod {clusterId:{clusterId}}) where p.updateTs<{ts} detach delete p", "clusterId",
					getClusterId(), "ts", ts);
		}
	}

	@Override
	public void scan() {
		scanCluster();
		scanNamespaces();
		scanNodes();
		scanPods();

	}

}
