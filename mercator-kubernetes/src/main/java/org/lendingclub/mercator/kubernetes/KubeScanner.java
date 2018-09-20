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

import static com.google.common.base.Strings.isNullOrEmpty;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.lendingclub.mercator.core.AbstractScanner;
import org.lendingclub.mercator.core.Scanner;
import org.lendingclub.mercator.core.ScannerBuilder;
import org.lendingclub.mercator.core.ScannerContext;
import org.lendingclub.mercator.core.SchemaManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

import io.fabric8.kubernetes.api.model.ComponentCondition;
import io.fabric8.kubernetes.api.model.ComponentStatus;
import io.fabric8.kubernetes.api.model.ContainerState;
import io.fabric8.kubernetes.api.model.ContainerStateRunning;
import io.fabric8.kubernetes.api.model.ContainerStateTerminated;
import io.fabric8.kubernetes.api.model.EndpointSubset;
import io.fabric8.kubernetes.api.model.Endpoints;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LoadBalancerStatus;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeSpec;
import io.fabric8.kubernetes.api.model.NodeStatus;
import io.fabric8.kubernetes.api.model.NodeSystemInfo;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.ServiceSpec;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.ReplicaSet;
import io.fabric8.kubernetes.api.model.extensions.ReplicaSetSpec;
import io.fabric8.kubernetes.api.model.extensions.ReplicaSetStatus;
import io.fabric8.kubernetes.client.KubernetesClient;

public class KubeScanner extends AbstractScanner {

	@Override
	public SchemaManager getSchemaManager() {
		return new KubeSchemaManager(getNeoRxClient());
	}

	static ObjectMapper mapper = new ObjectMapper();
	String clusterId;
	String clusterName;
	java.util.function.Supplier<KubernetesClient> clientSupplier;
	String alternateClusterIdAttributeName;
	static Logger logger = LoggerFactory.getLogger(KubeScanner.class);
	KubeEventScanner eventScanner;

	public KubeScanner(ScannerBuilder<? extends Scanner> builder) {
		super(builder);
		this.eventScanner = new KubeEventScanner(this);
		KubeScannerBuilder kubeScannerBuilder = KubeScannerBuilder.class.cast(builder);

		clientSupplier = KubeScannerBuilder.class.cast(builder).clientSupplier;
		this.clusterName = KubeScannerBuilder.class.cast(builder).clusterName;
		Preconditions.checkArgument(!Strings.isNullOrEmpty(kubeScannerBuilder.clusterName));
		Preconditions.checkState(getNeoRxClient() != null);


		if (!Strings.isNullOrEmpty(kubeScannerBuilder.clusterId)) {
			this.clusterId = kubeScannerBuilder.clusterId;
		}
		// We may have been configured with a clusterName, but no clusterId.
		// Here we attempt to look it up from a prior run.
		if (Strings.isNullOrEmpty(clusterId)) {
			logger.info("lookup up clusterId for {}", this.clusterName);
			KubeScannerBuilder ksb = KubeScannerBuilder.class.cast(builder);

			JsonNode cluster = getNeoRxClient().execCypher("match (a:KubeCluster {clusterName:{clusterName}}) return a",
					"clusterName", clusterName).blockingSingle(MissingNode.getInstance());

			if (cluster.isMissingNode()) {
				// there is no cluster by this name...we will create a new clusterId
				ksb.clusterId = "kc-" + Long.toHexString(new SecureRandom().nextLong());
			} else {

				if (Strings.isNullOrEmpty(cluster.path("clusterId").asText(null))) {
					// no clusterId has been set...go ahead and create one and set it
					ksb.clusterId = "kc-" + Long.toHexString(new SecureRandom().nextLong());
					getNeoRxClient().execCypher(
							"merge (a:KubeCluster {clusterName:{clusterName}}) set a.clusterId={clusterId}",
							"clusterName", clusterName, "clusterId", ksb.clusterId);
				} else {
					ksb.clusterId = cluster.path("clusterId").asText(null);
				}
			}
			this.clusterId = ksb.clusterId;
		}

		logger.info("clusterName={} clusterId={}", this.clusterName, this.clusterId);
		Preconditions.checkArgument(!Strings.isNullOrEmpty(this.clusterId), "clusterId must be set");
		Preconditions.checkArgument(!Strings.isNullOrEmpty(this.clusterName), "clusterName must be set");
	}

	public KubeEventScanner getEventScanner() {
		return eventScanner;
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
		return clientSupplier.get();
	}

	public void scanCluster() {

		Preconditions.checkArgument(!Strings.isNullOrEmpty(getClusterName()));
		Preconditions.checkArgument(!Strings.isNullOrEmpty(getClusterId()));

		ObjectNode props = mapper.createObjectNode();

		if (!Strings.isNullOrEmpty(alternateClusterIdAttributeName)) {
			// This is here to support trident which uses tridentClusterId
			// Sheesh...you would think that the SAME AUTHOR could get naming
			// standardized. Alas, it is a long story.

			props.put(alternateClusterIdAttributeName, getClusterId());
		}
		props.put("clusterName", clusterName);
		logger.info("scanning clusterId={} clusterName={} {}", getClusterId(), getClusterName());
		getNeoRxClient().execCypher("merge (a:KubeCluster {clusterId:{clusterId}})  "
				+ "on create set a+={props},a.url={url}, a.updateTs=timestamp(),a.createTs=timestamp() on match set a+={props},a.url={url}, a.updateTs=timestamp()",
				"clusterId", getClusterId(), "clusterName", getClusterName(), "url",
				getKubernetesClient().getMasterUrl().toString(), "props", props);

		scanComponentStatus();

	}

	private String mangleTagName(String key) {
		return "tag_"+key;
	}
	private String mangleLabelName(String key) {
		return "label_" + key;
	}

	private String mangleAnnotationName(String key) {
		return "annotation_" + key;
	}

	private void mapLabels(ObjectMeta md, ObjectNode target) {

		Set<String> tags = Sets.newHashSet();

		ArrayNode annotations = mapper.createArrayNode();
		target.set("annotations", annotations);
		Map<String, String> annotationMap = md.getAnnotations();
		if (annotationMap != null) {
			annotationMap.forEach((k, v) -> {
				target.put(mangleAnnotationName(k), v);
				target.put(mangleTagName(k), v);
				annotations.add(k);
				tags.add(k);
			});
		}

		ArrayNode labels = mapper.createArrayNode();
		target.set("labels", labels);
		Map<String, String> labelMap = md.getLabels();
		if (labelMap != null) {
			labelMap.forEach((k, v) -> {
				target.put(mangleLabelName(k), v);
				target.put(mangleTagName(k), v);
				labels.add(k);
				tags.add(k);
			});
		}


		ArrayNode tagsNode = mapper.createArrayNode();
		tags.forEach(it->{
			tagsNode.add(it);
		});
		target.set("tags", tagsNode);
	}

	private void purgeOldLabels(String nodeType, String idAttr, String idVal, ObjectNode targetNode, JsonNode actual) {
		Set<String> fieldsToRemove = Sets.newHashSet();

		actual.fieldNames().forEachRemaining(it -> {

			if (it.startsWith("label_")) {
				if (!targetNode.has(it)) {
					fieldsToRemove.add(it);
				}
			}
			if (it.startsWith("annotation_")) {
				if (!targetNode.has(it)) {
					fieldsToRemove.add(it);
				}
			}
			if (it.startsWith("tag_")) {
				if (!targetNode.has(it)) {
					fieldsToRemove.add(it);
				}
			}
		});

		if (!fieldsToRemove.isEmpty()) {
			List<String> tmp = Lists.newArrayList();
			fieldsToRemove.forEach(it -> {
				tmp.add("a.`" + it + "`");
			});
			String fields = Joiner.on(",").join(tmp);
			String cypher = "match (a:" + nodeType + " {" + idAttr + ":{idVal}}) REMOVE " + fields;

			logger.info("removing old labels for {} {}={}: {}", nodeType, idAttr, idVal, cypher);
			getNeoRxClient().execCypher(cypher, "idVal", idVal);
		}
	}

	void scanNamespace(Namespace ns) {

		ObjectNode n = mapper.createObjectNode();
		ObjectMeta md = ns.getMetadata();

		n.put("uid", ns.getMetadata().getUid());
		n.put("name", ns.getMetadata().getName());
		n.put("selfLink", md.getSelfLink());
		n.put("clusterId", getClusterId());

		mapLabels(md, n);

		getNeoRxClient()
				.execCypher("merge (a:KubeNamespace {uid:{uid}}) set a+={props}, a.updateTs=timestamp() return a",
						"uid", md.getUid(), "props", n)
				.forEach(it -> {
					purgeOldLabels("KubeNamespace", "uid", md.getUid(), n, it);
				});
	}

	public void scanNamespaces() {
		scanNamespaces(null);
	}

	public void scanNamespaces(String name) {

		List<Namespace> list = Lists.newArrayList();
		if (Strings.isNullOrEmpty(name)) {
			list = getKubernetesClient().namespaces().list().getItems();
		} else {
			Namespace ns = getKubernetesClient().namespaces().withName(name).get();
			if (ns != null) {
				list = ImmutableList.of(ns);
			}
		}
		getKubernetesClient().namespaces().list().getItems().forEach(it -> {
			scanNamespace(it);
		});
		getNeoRxClient().execCypher(
				"match (n:KubeNamespace {clusterId:{clusterId}}), (c:KubeCluster {clusterId:{clusterId}}) merge (c)-[x:CONTAINS]->(n)",
				"clusterId", getClusterId());
		deleteGarbage("KubeNamespace", null, name, 0);
	}

	private void scanPod(Pod pod) throws JsonProcessingException {

		long ts = getCurrentTimestamp();
		ObjectNode n = mapper.createObjectNode();
		applyMetadata(pod.getMetadata(), n);

		ObjectMeta meta = pod.getMetadata();

		PodSpec spec = pod.getSpec();

		n.put("restartPolicy", spec.getRestartPolicy());
		n.put("hostNetwork", spec.getHostNetwork());
		n.put("activeDeadlineSeconds", spec.getActiveDeadlineSeconds());
		n.put("automountServiceAccountToken", spec.getAutomountServiceAccountToken());
		n.put("dnsPolicy", spec.getDnsPolicy());
		n.put("hostIpc", spec.getHostIPC());
		n.put("hostname", spec.getHostname());
		n.put("hostPid", spec.getHostPID());
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

				"merge (p:KubePod {uid:{uid}}) on create set p={props},p.createTs=timestamp(), p.updateTs=timestamp() on match set p={props},p.updateTs=timestamp() return p",
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
				cs.put("startedAt", t.getStartedAt());
			}

			cs.put("clusterId", getClusterId());
			String containerId = it.getContainerID();
			if (!Strings.isNullOrEmpty(containerId)) {
				getNeoRxClient().execCypher(
						"merge (x:KubeContainer {containerId:{containerId}}) on match set x={props},x.updateTs=timestamp() on create set x={props}, x.updateTs=timestamp(), x.createTs=timestamp()",
						"containerId", containerId, "props", cs);
			}
		});

		List<String> containerNames = Lists.newArrayList();
		spec.getContainers().forEach(it -> {
			containerNames.add(it.getName());
			ObjectNode cspec = mapper.createObjectNode();
			ArrayNode args = mapper.createArrayNode();
			it.getArgs().forEach(arg -> {
				args.add(arg);
			});
			cspec.put("podUid", meta.getUid());
			cspec.put("name", it.getName());
			cspec.put("namespace", meta.getNamespace());
			cspec.put("clusterId", getClusterId());
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
							+ " on create set k={props}, k.updateTs=timestamp(),k.createTs=timestamp() "
							+ " on match  set k={props}, k.updateTs=timestamp()",
					"podUid", meta.getUid(), "name", it.getName(), "props", cspec);

			getNeoRxClient().execCypher(
					"match (c:KubeContainer {podUid:{uid},name:{name}}), (cs:KubeContainerSpec {podUid:{uid},name:{name}}) MERGE (c)-[x:USES]->(cs)",
					"uid", meta.getUid(), "name", it.getName());

		});

		getNeoRxClient().execCypher(
				"match (x:KubeContainerSpec {podUid:{podUid}}) where NOT x.name IN {list} detach delete x", "podUid",
				meta.getUid(), "list", containerNames);

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

		mapOwner(pod);
	}

	long getCurrentTimestamp() {
		return getNeoRxClient().execCypher("return timestamp()").blockingFirst().asLong();
	}

	private void applyMetadata(ObjectMeta metadata, ObjectNode target) {
		target.put("uid", metadata.getUid());
		target.put("resourceVersion", metadata.getResourceVersion());
		target.put("name", metadata.getName());
		target.put("namespace", metadata.getNamespace());
		target.put("clusterId", getClusterId());

		target.put("creationTimestamp", metadata.getCreationTimestamp());
		target.put("deletionTimestamp", metadata.getDeletionTimestamp());
		target.put("deletionGracePeriod", metadata.getDeletionGracePeriodSeconds());
		target.put("selfLink", metadata.getSelfLink());
		mapLabels(metadata, target);
	}

	private void scanNode(Node node) {
		ObjectNode n = mapper.createObjectNode();
		ObjectMeta meta = node.getMetadata();

		applyMetadata(meta, n);

		NodeSystemInfo nsi = node.getStatus().getNodeInfo();
		n.put("architecture", nsi.getArchitecture());
		n.put("bootId", nsi.getBootID());
		n.put("containerRuntimeVersion", nsi.getContainerRuntimeVersion());
		n.put("kernelVersion", nsi.getKernelVersion());
		n.put("kubeletVersion", nsi.getKubeletVersion());
		n.put("kubeProxyVersion", nsi.getKubeProxyVersion());
		n.put("machineId", nsi.getMachineID());
		n.put("operatingSystem", nsi.getOperatingSystem());
		n.put("osImage", nsi.getOsImage());
		n.put("systemUuid", nsi.getSystemUUID());

		NodeStatus ns = node.getStatus();

		NodeSpec nodeSpec = node.getSpec();

		n.put("externalId", nodeSpec.getExternalID());
		n.put("unschedulable", nodeSpec.getUnschedulable());
		n.put("podCidr", nodeSpec.getPodCIDR());
		n.put("providerId", nodeSpec.getProviderID());

		List<String> conditions = new ArrayList<>();
		if (ns != null) {
			if (ns.getAddresses() != null) {
				ns.getAddresses().forEach(addr -> {
					String type = com.google.common.base.Strings.nullToEmpty(addr.getType());
					if (type.equalsIgnoreCase("InternalIP")) {
						n.put("hostIp", addr.getAddress());
					} else if (type.equalsIgnoreCase("Hostname")) {
						n.put("hostname", addr.getAddress());
					}
				});
			}
			if (ns.getConditions() != null){
				ns.getConditions().forEach(cond->{
					String type = com.google.common.base.Strings.nullToEmpty(cond.getType());
					if (type.equalsIgnoreCase("Ready")){
						String status = (cond.getStatus().equalsIgnoreCase("true")) ? "Ready" : "Not Ready";
						n.put("status", status);
						n.put("lastHeartbeatTs", cond.getLastHeartbeatTime());
						n.put("lastTransactionTs", cond.getLastTransitionTime());
					}
                    conditions.add(String.format("%s:%s", cond.getType(), cond.getStatus()));
				});
			}
		}
		n.put("clusterId", clusterId);

		// Nodes are not guaranteed to be unique by name, so we use uid here
		getNeoRxClient().execCypher(
				"merge (n:KubeNode {uid:{uid}}) set n.clusterId={clusterId}, n={props}, n.conditions={cond}, n.updateTs=timestamp()",
				"uid", meta.getUid(), "props", n, "clusterId", clusterId, "cond", conditions).forEach(it -> {
					purgeOldLabels("KubeNode", "uid", meta.getUid(), n, it);
				});

	}

	public void scanNodes() {
		scanNodes(null);
	}

	public void scanNodes(String name) {
		ScannerContext sc = new ScannerContext();
		long ts = System.currentTimeMillis();
		sc.withName("KubeNode").exec(ctx -> {
			List<Node> nodes = ImmutableList.of();
			if (Strings.isNullOrEmpty(name)) {
				nodes = getKubernetesClient().nodes().list().getItems();
			} else {
				Node node = getKubernetesClient().nodes().withName(name).get();
				if (node != null) {
					nodes = ImmutableList.of(node);
				}
			}
			nodes.forEach(it -> {
				scanNode(it);
				sc.incrementEntityCount();
			});

		});

		getNeoRxClient().execCypher(
				"match (n:KubeNode {clusterId:{clusterId}}),(c:KubeCluster {clusterId:{clusterId}}) "

						+ " merge (c)-[x:CONTAINS]->(n) on create set n.createTs=timestamp()  set n.updateTs=timestamp()",
				"clusterId", clusterId);

		deleteGarbage("KubeNode", null, name, ts);
	}

	public void scanPods() {
		scanPods(null, null);
	}

	public void scanPods(String namespace) {
		scanPods(namespace, null);
	}

	public void scanPods(String namespace, String name) {

		ScannerContext sc = new ScannerContext();
		long ts = getCurrentTimestamp();
		sc.withName("KubePod").exec(ctx -> {
			List<Pod> podList = Lists.newArrayList();
			if (namespace == null && name == null) {
				podList = getKubernetesClient().pods().inAnyNamespace().list().getItems();
			} else if (namespace != null && name != null) {
				Pod pod = getKubernetesClient().pods().inNamespace(namespace).withName(name).get();
				if (pod != null) {
					podList.add(pod);
				}
			} else if (namespace != null && name == null) {
				podList = getKubernetesClient().pods().inNamespace(namespace).list().getItems();
			}
			podList.forEach(it -> {
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

		deleteGarbage("KubePod", namespace, name, ts);
	}

	protected void scanEndpoint(Endpoints endpoints) {

		ObjectNode mappedNode = mapper.createObjectNode();
		applyMetadata(endpoints.getMetadata(), mappedNode);

		ArrayNode tcpPorts = mapper.createArrayNode();
		ArrayNode udpPorts = mapper.createArrayNode();
		ArrayNode ipList = mapper.createArrayNode();
		ArrayNode nodeList = mapper.createArrayNode();
		mappedNode.set("ipAddresses", ipList);
		mappedNode.set("udpPorts", udpPorts);
		mappedNode.set("tcpPorts", tcpPorts);
		mappedNode.set("nodeNames", nodeList);
		List<ObjectReference> refList = Lists.newArrayList();
		List<EndpointSubset> subsetList = endpoints.getSubsets();
		if (subsetList != null) {
			subsetList.forEach(xx -> {

				xx.getAddresses().forEach(it2 -> {

					String ip = it2.getIp();
					ipList.add(ip);

					String nodeName = it2.getNodeName();
					if (nodeName != null) {
						nodeList.add(nodeName);
					}
					ObjectReference ref = it2.getTargetRef();
					if (ref != null) {
						refList.add(ref);

					}
				});

				xx.getPorts().forEach(it2 -> {
					if (it2.getProtocol().equalsIgnoreCase("TCP")) {
						tcpPorts.add(it2.getPort());
					} else if (it2.getProtocol().equalsIgnoreCase("UDP")) {
						udpPorts.add(it2.getPort());
					}
				});

			});
		}

		getNeoRxClient().execCypher("merge (e:KubeEndpoints {uid:{uid}}) set e={props}, e.updateTs=timestamp()", "uid",
				endpoints.getMetadata().getUid(), "props", mappedNode);
		getNeoRxClient().execCypher(
				"match (e:KubeEndpoints {uid:{uid},clusterId:{clusterId}}), (s:KubeService {clusterId:{clusterId},namespace:{namespace},name:{name}}) merge (s)-[x:CONTAINS]->(e)",
				"uid", endpoints.getMetadata().getUid(), "namespace", endpoints.getMetadata().getNamespace(), "name",
				endpoints.getMetadata().getName(), "clusterId", getClusterId());

		createPodEndpointRelationships(endpoints, refList);
		getNeoRxClient().execCypher(
				"match (e:KubeEndpoints {uid:{uid}}), (n:KubeNamespace {clusterId:{clusterId},name:{name}}) merge (n)-[x:CONTAINS]->(e) set x.updateTs=timestamp()",
				"uid", endpoints.getMetadata().getUid(), "clusterId", getClusterId(), "name",
				endpoints.getMetadata().getNamespace());
	}

	protected void createPodEndpointRelationships(Endpoints endpoints, List<ObjectReference> refList) {

		Set<String> graphRelatedPods = Sets.newHashSet();
		Set<String> kubeRelatedPods = Sets.newHashSet();
		Map<String, ObjectReference> podMap = Maps.newHashMap();
		// First, get all the existing relationships. This will make it easy to perform
		// an in-memory join, which will reduce
		// pressure on neo4j.
		getNeoRxClient().execCypher(
				"match (e:KubeEndpoints {uid:{uid}})--(p:KubePod) return p.uid as podUid, p.name as name, p.namespace as namespace",
				"uid", endpoints.getMetadata().getUid()).forEach(it -> {
					graphRelatedPods.add(it.path("podUid").asText());
				});

		// Now look for all the UIDs of pods that kube says are related to this
		// Endpoints object
		refList.forEach(it -> {
			if (it.getKind().equals("Pod")) {
				kubeRelatedPods.add(it.getUid());
				// keep a uid -> ObjectReference list
				podMap.put(it.getUid(), it);
			}
		});

		SetView<String> podRelationshipsToCreate = Sets.difference(kubeRelatedPods, graphRelatedPods);
		if (!podRelationshipsToCreate.isEmpty()) {
			logger.info("creating relationships from Endpoints->Pod: {} ", podRelationshipsToCreate);
		}
		podRelationshipsToCreate.forEach(podUid -> {

			ObjectReference pod = podMap.get(podUid);
			if (pod != null) {
				try {

					// make a best-effort to create the pod in case it does not yet exist
					scanPods(pod.getNamespace(), pod.getName());
				} catch (RuntimeException e) {
					logger.warn("problem scanning pod - " + e.toString());
				}
			}
			getNeoRxClient().execCypher(
					"match (e:KubeEndpoints {uid:{endpointUid}}),(x:KubePod {uid:{podUid}}) merge (e)-[r:ROUTES_TO]->(x)",
					"endpointUid", endpoints.getMetadata().getUid(), "podUid", podUid);
		});
		SetView<String> podRelationshipsToRemove = Sets.difference(graphRelatedPods, kubeRelatedPods);
		if (!podRelationshipsToRemove.isEmpty()) {
			logger.info("removing KubeEndpoints->KubePod relationships: {}", podRelationshipsToRemove);
		}
		getNeoRxClient().execCypher(
				"match (e:KubeEndpoints {uid:{endpointUid}})-[x]->(p:KubePod) where p.uid in {uidList} delete x",
				"endpointUid", endpoints.getMetadata().getUid(), "uidList",
				ImmutableList.copyOf(podRelationshipsToRemove));
	}

	public void scanEndpoints(String namespace, String name) {
		ScannerContext sc = new ScannerContext();
		long ts = System.currentTimeMillis();
		sc.withName("KubeEndpoints").exec(ctx -> {

			List<Endpoints> endpoints = Lists.newArrayList();
			if (namespace == null && name == null) {
				endpoints = getKubernetesClient().endpoints().inAnyNamespace().list().getItems();
			} else if (namespace != null && name == null) {
				endpoints = getKubernetesClient().endpoints().inNamespace(namespace).list().getItems();
			} else {
				Endpoints ep = getKubernetesClient().endpoints().inNamespace(namespace).withName(name).get();
				if (ep != null) {
					endpoints.add(ep);
				}

			}
			endpoints.forEach(it -> {
				try {
					scanEndpoint(it);
					sc.incrementEntityCount();
				} catch (Exception e) {
					maybeThrow(e);
				}
			});
		});

		deleteGarbage("KubeEndpoints", namespace, name, ts);
	}

	public void scanEndpoints() {
		scanEndpoints(null, null);
	}

	public void scanEndpoints(String namespace) {
		scanEndpoints(namespace, null);
	}

	public void scanDeployments() {
		scanDeployments(null, null);
	}

	public void scanDeployments(String namespace) {
		scanDeployments(namespace, null);
	}

	public void scanDeployments(String namespace, String name) {
		ScannerContext sc = new ScannerContext();
		long ts = getCurrentTimestamp();
		sc.withName("KubeDeployment").exec(ctx -> {
			List<Deployment> deployments = Lists.newArrayList();
			if (namespace == null && name == null) {
				deployments = getKubernetesClient().extensions().deployments().inAnyNamespace().list().getItems();
			} else if (namespace != null && name == null) {
				deployments = getKubernetesClient().extensions().deployments().inNamespace(namespace).list().getItems();
			} else {
				Deployment d = getKubernetesClient().extensions().deployments().inNamespace(namespace).withName(name)
						.get();
				if (d != null) {
					deployments.add(d);
				}
			}
			deployments.forEach(it -> {
				try {
					scanDeployment(it);
					sc.incrementEntityCount();
				} catch (Exception e) {
					maybeThrow(e);
				}
			});
			getNeoRxClient().execCypher(
					"match (c:KubeNamespace {clusterId:{clusterId}}), (p:KubeDeployment {clusterId:{clusterId}})  where p.namespace=c.name merge (c)-[x:CONTAINS]->(p) set x.updateTs=timestamp()",
					"clusterId", getClusterId());

		});
		deleteGarbage("KubeDeployment", namespace, name, ts);
	}

	private void scanDeployment(Deployment d) {
		ObjectNode n = mapper.createObjectNode();
		ObjectMeta meta = d.getMetadata();
		mapLabels(meta, n);

		n.put("creationTimestamp", meta.getCreationTimestamp());
		n.put("name", meta.getName());
		n.put("generateName", meta.getGenerateName());
		n.put("uid", meta.getUid());
		n.put("namespace", meta.getNamespace());
		n.put("clusterName", meta.getClusterName());
		n.put("clusterId", getClusterId());
		n.put("selfLink", meta.getSelfLink());

		getNeoRxClient().execCypher("merge (d:KubeDeployment {uid:{uid}}) set d={props},d.updateTs=timestamp()", "uid",
				meta.getUid(), "props", n);

		getNeoRxClient().execCypher(
				"match (c:KubeNamespace {clusterId:{clusterId}}), (p:KubeDeployment {clusterId:{clusterId}})  where p.namespace=c.name merge (c)-[x:CONTAINS]->(p) set x.updateTs=timestamp()",
				"clusterId", getClusterId());

	}

	public void scanReplicaSets() {
		scanReplicaSets(null, null);
	}

	public void scanReplicaSets(String namespace) {
		scanReplicaSets(namespace, null);
	}

	public void scanReplicaSets(String namespace, String name) {
		ScannerContext sc = new ScannerContext();
		long ts = getCurrentTimestamp();

		sc.withName("KubeReplicaSet").exec(ctx -> {
			List<ReplicaSet> replicaSets = Lists.newArrayList();

			if (namespace == null && name == null) {
				replicaSets = getKubernetesClient().extensions().replicaSets().inAnyNamespace().list().getItems();
			} else if (namespace != null && name == null) {
				replicaSets = getKubernetesClient().extensions().replicaSets().inNamespace(namespace).list().getItems();
			} else {
				ReplicaSet rs = getKubernetesClient().extensions().replicaSets().inNamespace(namespace).withName(name)
						.get();
				if (rs != null) {
					replicaSets = ImmutableList.of(rs);
				}
			}
			replicaSets.forEach(it -> {

				try {
					sc.incrementEntityCount();
					scanReplicaSet(it);
				} catch (Exception e) {
					maybeThrow(e);
				}
			});
			getNeoRxClient().execCypher(
					"match (c:KubeNamespace {clusterId:{clusterId}}), (p:KubeReplicaSet {clusterId:{clusterId}})  where p.namespace=c.name merge (c)-[x:CONTAINS]->(p) set x.updateTs=timestamp()",
					"clusterId", getClusterId());
		});
		deleteGarbage("KubeReplicaSet", namespace, name, ts);
	}

	private void scanReplicaSet(ReplicaSet rs) {
		ObjectNode n = mapper.createObjectNode();
		ObjectMeta meta = rs.getMetadata();
		mapLabels(meta, n);

		n.put("creationTimestamp", meta.getCreationTimestamp());
		n.put("name", meta.getName());
		n.put("generateName", meta.getGenerateName());
		n.put("uid", meta.getUid());
		n.put("namespace", meta.getNamespace());
		n.put("clusterName", meta.getClusterName());
		n.put("clusterId", getClusterId());
		n.put("selfLink", meta.getSelfLink());

		ReplicaSetSpec spec = rs.getSpec();
		n.put("specReplicas", spec.getReplicas());

		ReplicaSetStatus status = rs.getStatus();

		n.put("statusAvailableReplicas", status.getAvailableReplicas());
		n.put("statusReplicas", status.getReplicas());
		n.put("statusReadyRepliccas", status.getReadyReplicas());
		n.put("statusFullyLabeledReplicas", status.getFullyLabeledReplicas());
		n.put("statusObservedGeneration", status.getObservedGeneration());

		getNeoRxClient().execCypher("merge (rs:KubeReplicaSet {uid:{uid}}) set rs={props},rs.updateTs=timestamp()",
				"uid", meta.getUid(), "props", n);

		getNeoRxClient().execCypher(
				"match (c:KubeNamespace {clusterId:{clusterId}}), (p:KubeReplicaSet {clusterId:{clusterId}})  where p.namespace=c.name merge (c)-[x:CONTAINS]->(p) set x.updateTs=timestamp()",
				"clusterId", getClusterId());
		mapOwner(rs);

	}

	/**
	 * Relates the pod to its owner. The only valid owner at present is a
	 * ReplicaSet.
	 *
	 * @param p
	 */
	private void mapOwner(Pod p) {

		String podUid = p.getMetadata().getUid();
		p.getMetadata().getOwnerReferences().forEach(it -> {
			String ownerType = it.getKind();
			if (ownerType != null && ownerType.equals("ReplicaSet")) {
				if (!podToReplicaSetRelationshipExists(p)) {

					getNeoRxClient().execCypher(
							"match (p:KubePod {uid:{podUid}}),(rs:KubeReplicaSet {uid:{rsUid}}) merge (rs)-[x:CONTAINS]->(p)",
							"podUid", podUid, "rsUid", it.getUid());
				}

			}

		});
	}

	private boolean replicaSetToDeploymentRelationshipExists(ReplicaSet rs) {
		return false;
	}

	private boolean podToReplicaSetRelationshipExists(Pod p) {
		return false;
	}

	/**
	 * Relates the ReplicaSet to its owner. The only valid owner at present is a
	 * Deployment.
	 *
	 * @param p
	 */
	private void mapOwner(ReplicaSet rs) {
		// This will be a bit inefficient right now. Since this relationship mapping is
		// going to be a no-op most of the time,
		// we don't need to send each one to neo4j. We can have an in-memory cache and
		// only issue the update if the relationship
		// is not present.
		String rsUid = rs.getMetadata().getUid();
		rs.getMetadata().getOwnerReferences().forEach(it -> {
			String ownerType = it.getKind();

			if (ownerType != null && ownerType.equals("Deployment")) {
				if (!replicaSetToDeploymentRelationshipExists(rs)) {

					getNeoRxClient().execCypher(
							"match (d:KubeDeployment {uid:{deploymentUid}}),(rs:KubeReplicaSet {uid:{rsUid}}) merge (d)-[x:CONTAINS]->(rs)",
							"deploymentUid", it.getUid(), "rsUid", rsUid);
				}
			}
		});
	}

	private ArrayNode toArrayNode(List<String> s) {
		ArrayNode n = mapper.createArrayNode();
		if (s != null) {

			s.forEach(it -> {
				n.add(it);
			});
		}

		return n;
	}

	protected void scanService(Service service) {
		ObjectNode mappedNode = mapper.createObjectNode();
		ObjectMeta md = service.getMetadata();

		applyMetadata(md, mappedNode);

		mappedNode.put("kind", service.getKind());
		ServiceSpec spec = service.getSpec();

		mappedNode.put("clusterIp", service.getSpec().getClusterIP());
		mappedNode.put("externalName", spec.getExternalName());
		mappedNode.put("externalTrafficPolicy", spec.getExternalTrafficPolicy());

		mappedNode.put("healthCheckNodePort", service.getSpec().getHealthCheckNodePort());
		mappedNode.put("loadBalancerIp", spec.getLoadBalancerIP());
		mappedNode.put("sessionAffinity", spec.getSessionAffinity());

		mappedNode.put("type", service.getSpec().getType());

		mappedNode.set("externalIps", toArrayNode(spec.getExternalIPs()));
		mappedNode.set("loadBalancerSourceRanges", toArrayNode(spec.getLoadBalancerSourceRanges()));

		LoadBalancerStatus status = service.getStatus().getLoadBalancer();

		ArrayNode ingressIpList = mapper.createArrayNode();
		ArrayNode ingressHostnames = mapper.createArrayNode();
		mappedNode.set("ingressIpAddresses", ingressIpList);
		mappedNode.set("ingressHostNames", ingressHostnames);
		status.getIngress().forEach(it -> {
			if (it.getIp() != null) {
				ingressIpList.add(it.getIp());
			}
			if (it.getHostname() != null) {
				ingressHostnames.add(it.getHostname());
			}

		});
		List<ServicePort> ports = spec.getPorts();
		ArrayNode portNames = mapper.createArrayNode();
		mappedNode.set("portNames", portNames);
		if (ports != null) {
			ArrayNode portsJson = mapper.createArrayNode();
			for (ServicePort port : ports) {

				String name = ((port.getName() != null) ? port.getName() : port.getProtocol() + "_" + port.getPort());
				ObjectNode json = mapper.createObjectNode();
				json.put("name", name);
				json.put("nodePort", port.getNodePort());
				json.put("port", port.getPort());
				json.put("protocol", port.getProtocol());
				portsJson.add(json);

				portNames.add(name);

				mappedNode.put("port_" + name + "_port", port.getPort());
				mappedNode.put("port_" + name + "_nodePort", port.getNodePort());
				IntOrString intOrString = port.getTargetPort();
				if (intOrString != null) {

					if (intOrString.getIntVal() != null) {
						mappedNode.put("port_" + name + "_targetPort", intOrString.getIntVal());
					} else if (intOrString.getStrVal() != null) {
						mappedNode.put("port_" + name + "_targetPort", intOrString.getStrVal());
					}
				}
			}
			mappedNode.put("portsJson", portsJson.toString());
		}

		getNeoRxClient().execCypher(
				"merge (s:KubeService {name:{name},namespace:{namespace}}) set s={props}, s.updateTs=timestamp()",
				"name", mappedNode.get("name").asText(), "namespace", mappedNode.get("namespace").asText(), "props",
				mappedNode);

		getNeoRxClient().execCypher(
				"match (s:KubeService {name:{name},namespace:{namespace},clusterId:{clusterId}}),(n:KubeNamespace {name:{namespace}, clusterId:{clusterId}}) merge (n)-[x:CONTAINS]->(s) set x.updateTs=timestamp()",
				"name", mappedNode.get("name").asText(), "namespace", mappedNode.get("namespace").asText(), "clusterId",
				getClusterId());

		// scan endpoints for good measure
		scanEndpoints(service.getMetadata().getNamespace(), service.getMetadata().getName());

	}

	private void deleteOrphannedNodes(String... labels) {

		// get a list of all the clusterIds
		List<String> allClusters = getNeoRxClient()
				.execCypher("match (a:KubeCluster) return distinct a.clusterId as clusterId").map(it -> it.asText())
				.toList().blockingGet();

		for (String label : labels) {

			if (label.startsWith("Kube")) {
				// delete any nodes that have no relationships that are older than "tolerance"
				// milliseconds.
				// The tolerance protects against the case where we might be creating a node
				// concurrently.
				long tolerance = TimeUnit.SECONDS.toMillis(10);
				getNeoRxClient().execCypher(
						"match (a:" + label
								+ ") where not (a)--()  and timestamp()-a.updateTs > {tolerance} detach delete a",
						"tolerance", tolerance);

				getNeoRxClient().execCypher(
						"match (a:" + label
								+ ") where (not exists (a.clusterId)) or (not a.clusterId in {list}) detach delete a",
						"list", allClusters);
			}

		}

	}

	private void deleteMismatchedDependentNodes(String parent, String child) {
		getNeoRxClient().execCypher(
				"match (a:" + parent + ")--(x:" + child + ") where a.clusterId<>x.clusterId detach delete x");
	}

	public void cleanupData() {
		Stopwatch sw = Stopwatch.createStarted();
		// get rid of items with relationships with non-matching clusters
		deleteMismatchedDependentNodes("KubeCluster", "KubeNode");
		deleteMismatchedDependentNodes("KubeCluster", "KubeNamespace");
		deleteMismatchedDependentNodes("KubeNamespace", "KubeEndpoints");
		deleteMismatchedDependentNodes("KubeNamespace", "KubeDeployment");
		deleteMismatchedDependentNodes("KubeNamespace", "KubePod");
		deleteMismatchedDependentNodes("KubeNamespace", "KubeReplicaSet");
		deleteMismatchedDependentNodes("KubeNamespace", "KubeService");
		deleteOrphannedNodes("KubeContainerSpec", "KubeContainer", "KubeService", "KubePod", "KubeNode",
				"KubeDeployment", "KubeEndpoints", "KubeNamespace");

		logger.info("data cleanup took {}ms", sw.elapsed(TimeUnit.MILLISECONDS));

	}

	public void scanServices() {
		scanServices(null, null);
	}

	public void scanServices(String namespace) {
		scanServices(namespace, null);
	}

	public void scanComponentStatus() {
		try {
			List<ComponentStatus> csList = getKubernetesClient().componentstatuses().list().getItems();
			csList.forEach(it -> {
				String name = it.getMetadata().getName();
				ObjectNode props = mapper.createObjectNode();
				props.put("name", name);
				props.put("clusterId", getClusterId());
				props.put("clusterName", getClusterName());

				List<ComponentCondition> conditions = it.getConditions();
				if (conditions != null) {
					conditions.forEach(cc -> {
						props.put("message", cc.getMessage());
						props.put("error", cc.getError());
						props.put("status", cc.getStatus());
						props.put("type", cc.getType());
					});
				}
				getNeoRxClient().execCypher(
						"merge (c:KubeComponentStatus {clusterId:{clusterId},name:{name}}) set c={props},c.updateTs=timestamp()",
						"clusterId", getClusterId(), "name", name, "props", props);

			});
			getNeoRxClient().execCypher(
					"match (s:KubeComponentStatus {clusterId:{clusterId}}), (c:KubeCluster {clusterId:{clusterId}}) "
							+ "merge (c)-[x:HAS_STATUS]->(s)",
					"clusterId", getClusterId());
		} catch (Exception e) {
			maybeThrow(e);
		}
	}

	public void scanServices(String namespace, String name) {
		ScannerContext sc = new ScannerContext();
		long ts = getCurrentTimestamp();
		sc.withName("KubeService").exec(ctx -> {
			List<Service> serviceList = Lists.newArrayList();
			if (namespace == null && name == null) {
				serviceList = getKubernetesClient().services().inAnyNamespace().list().getItems();
			} else if (namespace != null && name == null) {
				serviceList = getKubernetesClient().services().inNamespace(namespace).list().getItems();
			} else if (namespace != null && name != null) {
				Service service = getKubernetesClient().services().inNamespace(namespace).withName(name).get();
				if (service != null) {
					serviceList.add(service);
				}
			}
			serviceList.forEach(it -> {
				try {
					sc.incrementEntityCount();
					scanService(it);
				} catch (Exception e) {
					maybeThrow(e);
				}
			});
		});
		deleteGarbage("KubeService", namespace, name, ts);
	}

	@Override
	public void scan() {
		cleanupData();
		scanCluster();
		scanComponentStatus();
		scanNamespaces();
		scanNodes();
		scanPods();
		scanDeployments();
		scanReplicaSets();
		scanServices();
		scanEndpoints();

	}

	void deleteNeo4jNode(JsonNode n) {

		logger.info("deleting node: {}", n);

		String uid = n.path("uid").asText();
		if (!Strings.isNullOrEmpty(uid)) {
			String cypher = String.format("match (a:%s {clusterId:{clusterId},uid:{uid}}) detach delete a",
					n.path("type").asText());
			getNeoRxClient().execCypher(cypher, "clusterId", getClusterId(), "uid", uid);
		} else {
			logger.error("node is missing uid: {}", n);
		}

	}

	/**
	 * Should delete ONLY has meaning/relevance in the context of maybeDelete(). If
	 * the metadata is null *OR* if the uid doesn't match the expected uid.
	 *
	 * @param md
	 * @param expectedUid
	 * @return
	 */
	static boolean shouldDelete(HasMetadata md, String expectedUid) {

		// some symbolic references to make the code more readable.
		// Note that these are not global to the classs since they only have meaning relative to the method
		final boolean DELETE=true;
		final boolean DO_NOT_DELETE=false;
		try {
			if (md == null) {
				// this means that the metadata wasn't located by name+namespace, so it should
				// be deleted
				return DELETE;
			}
			if (md.getMetadata() == null) {
				// shouldn't happen, but protect against NPE anyway.
				return DO_NOT_DELETE;
			}
			if (isNullOrEmpty(expectedUid)) {
				// if there is no expected uid, then we can't possibly say to delete the node
				return DO_NOT_DELETE;
			}

			String actualUid = md.getMetadata().getUid();
			if (Strings.isNullOrEmpty(actualUid)) {
				return DO_NOT_DELETE;
			}
			if (expectedUid.equals(actualUid)==false) {

				// If the uid of the object we found does not match the uid of the object we are expecting
				// then we should delete whatever we found.  This would happen, for instance, if an entity
				// was deleted and re-created with the same name
				return DELETE;
			}

		} catch (RuntimeException e) {
			// this should not happen and if it does, logic above should be ch
			logger.error("logic in shouldDelete() is not Exception-safe...please fix", e);
		}

		return DO_NOT_DELETE;
	}

	void maybeDelete(JsonNode n) {

		String name = n.path("name").asText();
		String namespace = n.path("namespace").asText();
		String type = n.path("type").asText();
		String expectedUid = n.path("uid").asText();

		if (logger.isDebugEnabled()) {
			logger.debug("delete candidate: {}", n);
		}
		if (type.equals("KubeNamespace")) {
			Namespace ns = getKubernetesClient().namespaces().withName(name).get();

			if (shouldDelete(ns, expectedUid)) {
				deleteNeo4jNode(n);
			}

		} else if (type.equals("KubeNode")) {

			Node node = getKubernetesClient().nodes().withName(name).get();

			if (shouldDelete(node, expectedUid)) {
				deleteNeo4jNode(n);
			}
		} else if (type.equals("KubePod")) {
			Pod pod = getKubernetesClient().pods().inNamespace(namespace).withName(name).get();
			if (shouldDelete(pod, expectedUid)) {
				deleteNeo4jNode(n);
			}
		} else if (type.equals("KubeEndpoints")) {
			Endpoints endpoints = getKubernetesClient().endpoints().inNamespace(namespace).withName(name).get();
			if (shouldDelete(endpoints, expectedUid)) {
				deleteNeo4jNode(n);
			}
		} else if (type.equals("KubeReplicaSet")) {
			ReplicaSet rs = getKubernetesClient().extensions().replicaSets().inNamespace(namespace).withName(name)
					.get();
			if (shouldDelete(rs, expectedUid)) {
				deleteNeo4jNode(n);
			}
		} else if (type.equals("KubeDeployment")) {
			Deployment deployment = getKubernetesClient().extensions().deployments().inNamespace(namespace)
					.withName(name).get();
			if (shouldDelete(deployment, expectedUid)) {
				deleteNeo4jNode(n);
			}
		} else if (type.equals("KubeService")) {
			Service service = getKubernetesClient().services().inNamespace(namespace).withName(name).get();
			if (shouldDelete(service, expectedUid)) {
				deleteNeo4jNode(n);
			}
		}

		else {
			logger.warn("garbage collection does not know how to handle type: " + type);
			return;
		}

	}

	protected void injectionCheck(String val) {
		if (val != null) {
			for (char c : val.toCharArray()) {
				if (Character.isLetterOrDigit(c) || c == '-' || c == '_' || c == '.') {
					// ok
				} else {
					throw new IllegalArgumentException("illegal character in: '" + val + "' (" + c + ")");
				}
			}
		}
	}

	void deleteGarbage(String type, String namespace, String name, long ts) {
		logger.info("garbage collecting type={} in clusterId={}", type, getClusterId());
		String whereClause = "";

		injectionCheck(type);
		injectionCheck(namespace);
		injectionCheck(name);
		if (ts > 0) {
			if (!Strings.isNullOrEmpty(whereClause)) {
				whereClause += " and ";
			}
			whereClause += " a.updateTs<" + ts + " ";
		}
		if (!Strings.isNullOrEmpty(namespace)) {
			if (!Strings.isNullOrEmpty(whereClause)) {

				whereClause += " and ";
			}
			whereClause += " a.namespace='" + namespace + "' ";
		}
		if (!Strings.isNullOrEmpty(name)) {
			if (!Strings.isNullOrEmpty(whereClause)) {
				whereClause += " and ";
			}
			whereClause += " a.name='" + name + "' ";
		}
		if (!Strings.isNullOrEmpty(whereClause)) {
			whereClause = " where " + whereClause;
		}
		String cypher = "match (a:" + type + " {clusterId:{clusterId}}) " + whereClause
				+ " return a.uid as uid, a.name as name, '" + type
				+ "' as type, a.clusterId as clusterId, a.namespace as namespace";

		if (logger.isDebugEnabled()) {
			logger.debug("cypher to collect GC candidates: {}", cypher);
		}

		getNeoRxClient().execCypher(cypher, "clusterId", getClusterId()).forEach(this::maybeDelete);
	}

}
