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
package org.macgyver.mercator.ucs;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.jdom2.Element;
import org.lendingclub.mercator.core.AbstractScanner;
import org.lendingclub.mercator.core.Scanner;
import org.lendingclub.mercator.core.ScannerBuilder;
import org.lendingclub.mercator.core.SchemaManager;
import org.lendingclub.neorx.NeoRxClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;

public class UCSScanner extends AbstractScanner  {

	Logger logger = LoggerFactory.getLogger(UCSScanner.class);

	UCSScannerBuilder ucsScannerBuilder;

	static ObjectMapper mapper = new ObjectMapper();

	AtomicReference<UCSClient> ucsClientRef = new AtomicReference<>();

	public UCSScanner(ScannerBuilder<? extends Scanner> builder, Map<String, String> props) {
		super(builder, props);

		this.ucsScannerBuilder = (UCSScannerBuilder) builder;

	}

	public UCSClient getUCSClient() {
		UCSClient client = ucsClientRef.get();
		if (client == null) {
			UCSClient.Builder ucsClientBuilder = new UCSClient.Builder();
			
			ucsClientBuilder = ucsClientBuilder.withCertValidationEnabled(ucsScannerBuilder.validateCertificates);
			
		
			if (ucsScannerBuilder.okRestConfig!=null) {
				ucsClientBuilder.okRestConfigurer = ucsScannerBuilder.okRestConfig;
				
			}
			if (ucsScannerBuilder.okHttpConfig!=null) {
				ucsClientBuilder.okHttpConfigurator = ucsScannerBuilder.okHttpConfig;
			}
			client = ucsClientBuilder.withUrl(ucsScannerBuilder.url).withUsername(ucsScannerBuilder.username)
					.withPassword(ucsScannerBuilder.password).build();
			ucsClientRef.set(client);
		}
		return client;
	}

	ObjectNode toJson(Element element) {
		ObjectNode n = mapper.createObjectNode();

		element.getAttributes().forEach(it -> {
			n.put(it.getName(), it.getValue());
		});
		n.put("ucsManagerId", getUCSClient().getUCSManagerId());
		return n;
	}

	protected void recordManagementController() {
		String id = getUCSClient().getUCSManagerId();

		String cypher = "merge (m:UCSManager {mercatorId:{mercatorId}}) set m+={props}, m.updateTs=timestamp()";

		ObjectNode props = mapper.createObjectNode();
		props.put("url", getUCSClient().target.getUrl());
		getProjector().getNeoRxClient().execCypher(cypher, "mercatorId", id, "props", props);

	}

	protected void recordChassis(Element element) {

		ObjectNode n = toJson(element);

		String serial = n.get("serial").asText();
		String mercatorId = Hashing.sha1().hashString(serial, Charsets.UTF_8).toString();
		n.put("mercatorId", mercatorId);
		String cypher = "merge (c:UCSChassis {mercatorId:{mercatorId}}) set c+={props}, c.updateTs=timestamp()";

		getProjector().getNeoRxClient().execCypher(cypher, "mercatorId", mercatorId, "props", n);

		cypher = "match (m:UCSManager {mercatorId:{managerId}}), (c:UCSChassis {mercatorId:{chassisId}}) merge (m)-[r:MANAGES]->(c)";

		getProjector().getNeoRxClient().execCypher(cypher, "managerId", getUCSClient().getUCSManagerId(), "chassisId",
				mercatorId);

		getUCSClient().resolveChildren(n.get("dn").asText(), "computeBlade").forEach(it-> {
			recordComputeBlade(mercatorId,it);
			
		});
		
	}

	protected void recordComputeBlade(String chassisMercatorId, Element element) {
		ObjectNode n = toJson(element);

		String mercatorId = n.get("uuid").asText();
		n.put("mercatorId", mercatorId);
		String cypher = "merge (c:UCSComputeBlade {mercatorId:{mercatorId}}) set c+={props}, c.updateTs=timestamp()";

		getProjector().getNeoRxClient().execCypher(cypher, "mercatorId", mercatorId, "props", n);

		cypher = "match (c:UCSChassis {mercatorId:{chassisMercatorId}}), (b:UCSComputeBlade {mercatorId:{bladeId}}) merge (c)-[r:CONTAINS]->(b) set r.updateTs=timestamp()";

		getProjector().getNeoRxClient().execCypher(cypher, "chassisMercatorId", chassisMercatorId, "bladeId",
				mercatorId);	
		
		recordServerServiceProfileRelationship("UCSComputeBlade",n);
		
	}
	protected void recordComputeRackUnit(Element element) {

		ObjectNode n = toJson(element);

		String mercatorId = n.get("uuid").asText();
		n.put("mercatorId", mercatorId);
		String cypher = "merge (c:UCSComputeRackUnit {mercatorId:{mercatorId}}) set c+={props}, c.updateTs=timestamp()";

		getProjector().getNeoRxClient().execCypher(cypher, "mercatorId", mercatorId, "props", n);

		cypher = "match (m:UCSManager {mercatorId:{managerId}}), (c:UCSComputeRackUnit {mercatorId:{rackUnitId}}) merge (m)-[r:MANAGES]->(c) set r.updateTs=timestamp()";

		getProjector().getNeoRxClient().execCypher(cypher, "managerId", getUCSClient().getUCSManagerId(), "rackUnitId",
				mercatorId);

		recordServerServiceProfileRelationship("UCSComputeRackUnit",n);
	}
	
	protected void recordServerServiceProfileRelationship(String label, ObjectNode n) {
		String serviceProfileDn = n.path("assignedToDn").asText();
		String serviceProfileMercatorId = computeMercatorIdFromDn(serviceProfileDn);
		String serverMercatorId = n.path("mercatorId").asText();
		String cypher = "match (p:UCSServerServiceProfile {mercatorId:{profileMercatorId}}), (s:"+label+" {mercatorId:{serverMercatorId}}) merge (s)-[r:USES]->(p) set r.updateTs=timestamp()";
		
		getProjector().getNeoRxClient().execCypher(cypher, "profileMercatorId",serviceProfileMercatorId,"serverMercatorId",serverMercatorId);
	}

	protected void recordFabricInterconnect(Element element) {
		ObjectNode n = toJson(element);

		if (n.path("model").asText().toUpperCase().startsWith("UCS-FI-")) {
			String serial = n.get("serial").asText();
			String mercatorId = Hashing.sha1().hashString(serial, Charsets.UTF_8).toString();
			n.put("mercatorId", mercatorId);

			String cypher = "merge (c:UCSFabricInterconnect {mercatorId:{mercatorId}}) set c+={props}, c.updateTs=timestamp()";

			getProjector().getNeoRxClient().execCypher(cypher, "mercatorId", mercatorId, "props", n);

			cypher = "match (m:UCSManager {mercatorId:{managerId}}), (c:UCSFabricInterconnect {mercatorId:{fiId}}) merge (m)-[r:MANAGES]->(c) set r.updateTs=timestamp()";

			getProjector().getNeoRxClient().execCypher(cypher, "managerId", getUCSClient().getUCSManagerId(), "fiId",
					mercatorId);
			
			
		}
	}

	protected static String extractOrgDn(String dn) {
		List<String> orgList = Lists.newArrayList();
		Splitter.on("/").splitToList(dn).forEach(it ->{
			if (it.startsWith("org-")) {
				orgList.add(it);
			}
		});
		return Joiner.on("/").join(orgList);	
	}

	
	protected void recordServerServiceProfile(Element element) {
		ObjectNode n = toJson(element);

		
		String mercatorId = computeMercatorIdFromDn(n.get("dn").asText());
		
		String cypher = "merge (p:UCSServerServiceProfile {mercatorId:{mercatorId}}) set p+={props}, p.updateTs=timestamp()";
		
		getProjector().getNeoRxClient().execCypher(cypher, "mercatorId",mercatorId,"props",n);
		
		String orgDn = extractOrgDn(n.get("dn").asText());
		String orgMercatorId = computeMercatorIdFromDn(orgDn);
		
		cypher = "match (e:UCSServerServiceProfile {mercatorId:{mercatorId}}), (o:UCSOrg {mercatorId:{orgMercatorId}}) merge (o)-[r:CONTAINS]->(e) set r.updateTs=timestamp()";
		
		getProjector().getNeoRxClient().execCypher(cypher, "mercatorId",mercatorId,"orgMercatorId",orgMercatorId);
	}
	String computeMercatorIdFromDn(String dn) {
		return Hashing.sha1().hashString(qualifyDn(dn), Charsets.UTF_8).toString();
	}
	String qualifyDn(String dn) {
		return getUCSClient().getUCSManagerId()+"@"+dn;
	}
	protected void recordOrg(Element element) {
		getUCSClient().logDebug("org", element);
		
		ObjectNode n = toJson(element);

	
		String mercatorId = computeMercatorIdFromDn(n.get("dn").asText());
		n.put("mercatorId", mercatorId);
		
		String cypher = "merge (c:UCSOrg {mercatorId:{mercatorId}}) set c+={props}, c.updateTs=timestamp()";

		getProjector().getNeoRxClient().execCypher(cypher, "mercatorId", mercatorId, "props", n);
		
		cypher = "match (m:UCSManager {mercatorId:{managerId}}), (c:UCSOrg {mercatorId:{orgId}}) merge (m)-[r:MANAGES]->(c)";

		getProjector().getNeoRxClient().execCypher(cypher, "managerId", getUCSClient().getUCSManagerId(), "orgId",
				mercatorId);
		
		
		
	}
	
	protected void recordUCSManagerRelationship(String label) {
		String cypher = "match (e:"+label+"), (m:UCSManager {mercatorId:{ucsManagerId}}) merge (m)-[r:MANAGES]->(e) set r.updateTs=timestamp()";
		getProjector().getNeoRxClient().execCypher(cypher, "ucsManagerId",getUCSClient().getUCSManagerId());
	}
	protected void recordFabricExtender(Element element) {

		ObjectNode n = toJson(element);

		String serial = n.get("serial").asText();
		String mercatorId = Hashing.sha1().hashString(serial, Charsets.UTF_8).toString();
		n.put("mercatorId", mercatorId);

		String cypher = "merge (c:UCSFabricExtender {mercatorId:{mercatorId}}) set c+={props}, c.updateTs=timestamp()";

		getProjector().getNeoRxClient().execCypher(cypher, "mercatorId", mercatorId, "props", n);

		cypher = "match (m:UCSManager {mercatorId:{managerId}}), (c:UCSFabricExtender {mercatorId:{fexId}}) merge (m)-[r:MANAGES]->(c)";

		getProjector().getNeoRxClient().execCypher(cypher, "managerId", getUCSClient().getUCSManagerId(), "fexId",
				mercatorId);

	}


	public void scan() {
		try {
			getUCSClient().getToken();
			
			logger.info("scanning UCSManagementController...");
			recordManagementController();

			logger.info("scanning UCSOrg...");
			getUCSClient().resolveClass("orgOrg").forEach(it->{
				recordOrg(it);				
			});
			recordUCSManagerRelationship("UCSOrg");
					
			logger.info("scanning UCSServerServiceProfile...");
			getUCSClient().resolveClass("lsServer").forEach(it->{
				recordServerServiceProfile(it);
			});
			recordUCSManagerRelationship("UCSServerServiceProfile");
			
			logger.info("scanning UCSComputeRackUnit...");
			getUCSClient().resolveClass("computeRackUnit").forEach(it -> {
				recordComputeRackUnit(it);
			});
			recordUCSManagerRelationship("UCSComputeRackUnit");
			
			logger.info("scanning UCSChassis...");
			getUCSClient().resolveClass("equipmentChassis").forEach(it -> {
				recordChassis(it);				
			});
			recordUCSManagerRelationship("UCSChassis");
			
			logger.info("scanning UCSFabricExtender...");
			getUCSClient().resolveClass("equipmentFex").forEach(it -> {
				recordFabricExtender(it);
			});
			recordUCSManagerRelationship("UCSFabricExtender");
						
			logger.info("scanning UCSFabricInterconnect...");
			getUCSClient().resolveClass("networkElement").forEach(it -> {
				recordFabricInterconnect(it);
			});
			recordUCSManagerRelationship("UCSFabricInterconnect");
			
		} finally {
			getUCSClient().logout();
		}
	}

	public SchemaManager getSchemaManager() {
		UCSSchemaManager m = new UCSSchemaManager(getProjector().getNeoRxClient());
		return m;
	}
	

	class UCSSchemaManager extends SchemaManager {

		public UCSSchemaManager(NeoRxClient client) {
			super(client);

		}

		@Override
		public void applyConstraints() {
			applyUniqueConstraint("UCSFabricExtender","mercatorId");
			applyUniqueConstraint("UCSManager","mercatorId");
			applyUniqueConstraint("UCSComputeBlade","mercatorId");
			applyUniqueConstraint("UCSComputeRackUnit","mercatorId");
			applyUniqueConstraint("UCSChassis","mercatorId");
			applyUniqueConstraint("UCSFabricInterconnect","mercatorId");
			applyUniqueConstraint("UCSOrg","mercatorId");
			applyUniqueConstraint("UCSServerServiceProfile","mercatorId");
			
		}

	}
}
