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

package org.lendingclub.mercator.vmware;

import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;

import org.lendingclub.mercator.core.AbstractScanner;
import org.lendingclub.mercator.core.SchemaManager;
import org.lendingclub.neorx.NeoRxClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.hash.Hashing;
import com.vmware.vim25.GuestInfo;
import com.vmware.vim25.HostHardwareInfo;
import com.vmware.vim25.ManagedObjectReference;
import com.vmware.vim25.VirtualMachineConfigInfo;
import com.vmware.vim25.mo.ClusterComputeResource;
import com.vmware.vim25.mo.Datacenter;
import com.vmware.vim25.mo.HostSystem;
import com.vmware.vim25.mo.ManagedEntity;
import com.vmware.vim25.mo.ServerConnection;
import com.vmware.vim25.mo.ServiceInstance;
import com.vmware.vim25.mo.VirtualMachine;

public class VMWareScanner extends AbstractScanner {

	ObjectMapper mapper = new ObjectMapper();
	Logger logger = LoggerFactory.getLogger(VMWareScanner.class);

	String vcenterUuid;

	Supplier<ServiceInstance> serviceInstanceSupplier;



	public VMWareScanner(VMWareScannerBuilder builder, Map<String, String> config) {
		super(builder,config);
		
		Map<String, String> cfg = new HashMap<String, String>();
		cfg.putAll(builder.getProjector().getProperties());
		cfg.putAll(config);
	
		this.serviceInstanceSupplier = Suppliers.memoize(new ServiceInstanceSupplier(cfg));
	}

	protected JsonNode ensureController() {
		String cypher = "merge (c:ComputeController:VMWareVCenter {id: {id}}) set c.type='vcenter' return c";
		return getProjector().getNeoRxClient().execCypher(cypher, "id", getVCenterId()).blockingFirst();
	}

	
	public synchronized String getVCenterId() {
		if (vcenterUuid == null) {
			vcenterUuid = getServiceInstance().getAboutInfo().getInstanceUuid();
		}
		return vcenterUuid;
	}

	private void setVal(ObjectNode n, String prop, String val) {
		n.put(prop, val);
	}
	private void setVal(ObjectNode n, String prop, boolean val) {
		n.put(prop, val);
	}


	protected void updateComputeHost(ObjectNode n) {
	
	
		String cypher ="merge (c:ComputeHost:VMWareHost {id:{id}}) on match set c+={p} ,c.updateTs=timestamp() ON CREATE SET c+={p}, c.updateTs=timestamp() return c";

		JsonNode computeHost = getProjector().getNeoRxClient().execCypher(cypher,"id", n.path("id").asText(),"p",n).blockingFirst();

	}

	protected void updateCluster(ObjectNode cluster) {

		String cypher = "merge (c:VMWareCluster {id:{id}}) on match set c+={props} "
				+ ",c.updateTs=timestamp() ON CREATE SET "
				+" c+={props}, c.updateTs=timestamp() return c";
		getProjector().getNeoRxClient().execCypher(cypher, "id",cluster.path("id").asText(),"props",cluster).blockingFirst();
	}

	protected void updateComputeInstance(ObjectNode n) {

	
		String cypher = "merge (c:ComputeInstance:VMWareGuest {id:{id}}) on match set "
				+ "c+={props} "
				+ ",c.updateTs=timestamp() ON CREATE SET "
				+ "c+={props}, c.updateTs=timestamp() return c";

		getProjector().getNeoRxClient().execCypher(cypher, "id",n.path("id").asText(),"props",n).blockingFirst();

	}

	String getUniqueId(VirtualMachine vm) {
		return vm.getConfig().getInstanceUuid();
	}

	String getUniqueId(ClusterComputeResource ccr) {
		return computeUniqueId(ccr.getMOR());
	}

	public String computeUniqueId(ManagedObjectReference mor) {
		Preconditions
				.checkNotNull(mor, "ManagedObjectReference cannot be null");

		Preconditions.checkArgument(
				!ManagedObjectTypes.VIRTUAL_MACHINE.equals(mor.getType()),
				"cannot call computeMacId() with mor.type=VirtualMachine");

		return Hashing
				.sha1()
				.hashString(getVCenterId() + mor.getType() + mor.getVal(),
						Charsets.UTF_8)
				.toString();
	}

	ObjectNode toComputeNodeData(VirtualMachine vm) {
		ObjectNode n = mapper.createObjectNode();
		try {
			VirtualMachineConfigInfo cfg = vm.getConfig();

			// http://www.virtuallyghetto.com/2011/11/vsphere-moref-managed-object-reference.html

			ServerConnection sc = vm.getServerConnection();

			ManagedObjectReference mor = vm.getMOR();
			String moType = mor.getType();
			String moVal = mor.getVal();
			GuestInfo g = vm.getGuest();
			setVal(n, "name", vm.getName());
			setVal(n, "id", getUniqueId(vm));
			setVal(n, "vmw_instanceUuid", cfg.getInstanceUuid());
			setVal(n, "vmw_morVal", moVal);
			setVal(n, "vmw_morType", moType);
			setVal(n, "vmw_annotation", cfg.getAnnotation());
			setVal(n, "vmw_guestToolsVersion", g.getToolsVersion());
			setVal(n, "vmw_guestId", g.getGuestId());
			setVal(n, "vmw_guestFamily", g.getGuestFamily());
			setVal(n, "vmw_guestFullName", g.getGuestFullName());
			setVal(n, "vmw_guestIpAddress", g.getIpAddress());
			setVal(n, "vmw_guestId", g.getGuestId());
			setVal(n, "vmw_guestHostName", g.getHostName());
			setVal(n, "vmw_guestAlternateName", cfg.getAlternateGuestName());
			setVal(n, "vmw_guestToolsRunningStatus",""+g.getToolsRunningStatus());
			setVal(n, "vmw_guestState",""+g.getGuestState());
			
			setVal(n, "vmw_locationId", cfg.getLocationId());
			setVal(n, "vmw_memoryMB", "" + cfg.getHardware().getMemoryMB());
			setVal(n, "vmw_numCPU", "" + cfg.getHardware().getNumCPU());
			setVal(n, "vmw_isTemplate",vm.getConfig().isTemplate());
			setVal(n, "vmw_powerState",vm.getSummary().getRuntime().getPowerState().toString());
			
		

		} catch (Exception e) {
			logger.warn("", e);
		}
		return n;
	}

	protected void scan(VirtualMachine vm) {
		logger.debug("scanning vm: {}", vm.getName());
		ObjectNode n = toComputeNodeData(vm);
		updateComputeInstance(n);
	}

	ObjectNode toObjectNode(ClusterComputeResource ccr) {

		ObjectNode n = mapper.createObjectNode().put("vmw_name", ccr.getName()).put("id",
				computeUniqueId(ccr.getMOR()));

		return n;
	}

	ObjectNode toObjectNode(HostSystem host) {
		ManagedObjectReference mor = host.getMOR();

		HostHardwareInfo hh = host.getHardware();
		ObjectNode n = mapper.createObjectNode().put("id", getUniqueId(host))
				.put("name", host.getName()).put("vmw_morType", mor.getType())
				.put("vmw_morVal", mor.getVal()).put("vmw_hardwareModel",

						hh.getSystemInfo().getModel())
				.put("vmw_cpuCoreCount", hh.getCpuInfo().getNumCpuCores())
				.put("vmw_memorySize", hh.getMemorySize());

		return n;
	}

	protected void updateClusterHostRelationship(ClusterComputeResource cluster, HostSystem host) {
		logger.debug("updating relationship between cluster={} and host={}",
				cluster.getName(), host.getName());
		String cypher = "match (h {id:{hostId} }), (c:VMWareCluster {id: {clusterId}}) MERGE (c)-[r:CONTAINS]->(h) ON CREATE SET r.updateTs=timestamp(),r.createTs=timestamp() ON MATCH SET r.updateTs=timestamp() return r";
		getProjector().getNeoRxClient().execCypher(cypher, "hostId", getUniqueId(host), "clusterId",
				getUniqueId(cluster));
	}

	protected void updateHostVmRelationship(HostSystem h, VirtualMachine vm) {

		logger.debug("updating relationship between host={} and vm={}",
				h.getName(), vm.getName());
		String cypher = "match (h:ComputeHost {id:{hostId} }), (c:ComputeInstance {id: {computeId}}) MERGE (h)-[r:HOSTS]->(c) ON CREATE SET r.updateTs=timestamp(),r.createTs=timestamp() ON MATCH SET r.updateTs=timestamp() return r";
		getProjector().getNeoRxClient().execCypher(cypher, "hostId", getUniqueId(h), "computeId",
				getUniqueId(vm));
	

	}

	protected void updateDatacenterClusterRelationship(Datacenter dc, ClusterComputeResource cluster) {

		logger.debug("updating relationship between dc={} and cluster={}",
				dc.getName(), cluster.getName());
		String cypher = "match (d:VMWareDatacenter {id:{datacenterId} }), (c:VMWareCluster {id: {clusterId}}) MERGE (d)-[r:CONTAINS]->(c) ON CREATE SET r.updateTs=timestamp(),r.createTs=timestamp() ON MATCH SET r.updateTs=timestamp() return r";
		getProjector().getNeoRxClient().execCypher(cypher, "datacenterId", computeUniqueId(dc.getMOR()), "clusterId",
				computeUniqueId(cluster.getMOR()));

		
	
		JsonNode vcenter = ensureController();
		String vcenterid = vcenter.get("id").asText();
		cypher = "match (c:ComputeController:VMWareVCenter {id:{vcenterId}}), (dc:VMWareDatacenter {id:{datacenterId} }) MERGE (c)-[r:MANAGES]->(dc) ON CREATE SET r.updateTs=timestamp() ON MATCH SET r.updateTs=timestamp() return r";
		getProjector().getNeoRxClient().execCypher(cypher, "vcenterId", vcenterid, "datacenterId",
				computeUniqueId(dc.getMOR()));
	}

	protected ObjectNode toObjectNode(Datacenter dc) {
		ObjectNode n = mapper.createObjectNode();
		n.put("id", computeUniqueId(dc.getMOR()));
		n.put("name", dc.getName());
		return n;
	}

	protected void scanDatacenter(Datacenter dc) {
		try {
			logger.info("scanning DataCenter: " + dc.getName());
			ObjectNode dataCenterNode = toObjectNode(dc);
			String cypher = "merge (d:VMWareDatacenter {id:{id}}) set d+={props}";

			getProjector().getNeoRxClient().execCypher(cypher, "id", dataCenterNode.path("id").asText(), "props", dataCenterNode);

			for (ManagedEntity me : dc.getHostFolder().getChildEntity()) {
				if (me instanceof ClusterComputeResource) {
					ClusterComputeResource cluster = (ClusterComputeResource) me;
					scanCluster(cluster);
					updateDatacenterClusterRelationship(dc, cluster);
				}
			}
		} catch (Exception e) {
			logger.warn("problem scanning datacenter: " + dc.getName(), e);
		}

	}

	public void scanAllDatacenters() {
		newQueryTemplate().findAllDatacenters().forEach(dc -> {
			scanDatacenter(dc);
		});
	}

	protected void scanCluster(ClusterComputeResource cluster) {
		logger.info("scanning cluster={}",cluster.getName());
		ObjectNode n = toObjectNode(cluster);
		updateCluster(n);

		for (HostSystem host : cluster.getHosts()) {
			scanHost(host, false);
			updateClusterHostRelationship(cluster, host);
		}

	}

	protected void scanHost(HostSystem host, boolean scanGuests) {
		try {
			logger.info("scanning esxi host={}",
					host.getName());
			ObjectNode n = toObjectNode(host);

			updateComputeHost(n);

			long now = getProjector().getNeoRxClient().execCypher("return timestamp() as ts")
					.blockingFirst().asLong();

			if (scanGuests) {
				logger.info("scanning guests on esxi host={}",host.getName());
				VirtualMachine[] vms = host.getVms();
				if (vms != null) {
					int count =0;
					for (VirtualMachine vm : vms) {
						count++;
						try {
							scan(vm);
						} catch (RuntimeException e) {
							logger.warn("problem scanning vm="+vm.getName(), e);
						}

						// Use a separate try/catch for the relationships so
						// that if
						// something went wrong above, we still get the
						// relationships right.
						try {
							updateHostVmRelationship(host, vm);
						} catch (RuntimeException e) {
							logger.warn("problem updating host-to-vm relationship", e);
						}
						if (count %10==0) {
							logger.info("scanned {} guests on host={}",count,host.getName());
						}
					}
					logger.info("scanned {} guests on host={}",count,host.getName());
				}

				clearStaleRelationships(host, now);
			}
		} catch (RemoteException e) {
			throw new VMWareExceptionWrapper(e);
		}

	}

	protected String getUniqueId(HostSystem h) {
		return computeUniqueId(h.getMOR());

	}

	protected void clearStaleRelationships(HostSystem host, long ts) {
		logger.info(
				"clearing stale ComputeHost->ComputeInstance relationships for host: {}",
				host.getName());
		String cypher = "match (h:ComputeHost {id:{id}})-[r:HOSTS]-(c:ComputeInstance) where r.updateTs<{ts} delete r";

		// this is for logging only

		for (JsonNode n : getProjector().getNeoRxClient()
				.execCypher(cypher.replace("delete r", "return r"), "id", getUniqueId(host), "ts", ts)
				.toList().blockingGet()) {
			logger.info("clearing stale relationship: {}", n);
		}
		// end of logging section

		getProjector().getNeoRxClient().execCypher(cypher, "id", getUniqueId(host), "ts", ts);
	}

	public void scanAllClusters() {
		VMWareQueryTemplate t = new VMWareQueryTemplate(getServiceInstance());
		for (ClusterComputeResource ccr : t.findAllClusters()) {
			try {
				scanCluster(ccr);
			} catch (RuntimeException e) {
				logger.warn("scan cluster failed: " + ccr.getName(), e);
			}
		}
	}

	public void scanAllHosts() {
		VMWareQueryTemplate t = new VMWareQueryTemplate(getServiceInstance());
		for (HostSystem host : t.findAllHostSystems()) {

			try {

				scanHost(host, true);

			} catch (RuntimeException e) {
				logger.warn("scan host failed: {}", host.getName());
			}

		}
	}

	@Override
	public void scan() {
		scanAllDatacenters();
		scanAllHosts();
	}

	

	public ServiceInstance getServiceInstance() {
		return serviceInstanceSupplier.get();
	}


	public VMWareQueryTemplate newQueryTemplate() {
		return new VMWareQueryTemplate(getServiceInstance());
	}

	@Override
	public SchemaManager getSchemaManager() {
		return new VMWareSchemaManager(getProjector().getNeoRxClient());
	}
	
	class VMWareSchemaManager extends SchemaManager {

		public VMWareSchemaManager(NeoRxClient client) {
			super(client);
			
		}
		
		public void applyConstraints() {
			applyConstraint("CREATE  CONSTRAINT ON (a:VMWareHost) ASSERT a.id IS UNIQUE ");
			applyConstraint("CREATE  CONSTRAINT ON (a:VMWareDatacenter) ASSERT a.id IS UNIQUE ");
			applyConstraint("CREATE  CONSTRAINT ON (a:VMWareCluster) ASSERT a.id IS UNIQUE ");
			applyConstraint("CREATE  CONSTRAINT ON (a:VMWareVCenter) ASSERT a.id IS UNIQUE ");
		
		}
		
	}
	
}
