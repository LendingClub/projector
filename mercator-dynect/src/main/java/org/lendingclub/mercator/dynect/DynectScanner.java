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
package org.lendingclub.mercator.dynect;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.lendingclub.mercator.core.AbstractScanner;
import org.lendingclub.mercator.core.Scanner;
import org.lendingclub.mercator.core.ScannerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

public class DynectScanner extends AbstractScanner {
	
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	private Supplier<DynectClient> clientSupplier = Suppliers.memoize(new DynectClientSupplier());
	private ObjectMapper mapper = new ObjectMapper();



	public DynectScanner(ScannerBuilder<? extends Scanner> builder) {
		super(builder);
	}
	
	public DynectClient getDynectClient() {
		return clientSupplier.get();
	}

	@Override
	public void scan() {
		
		List<String> zones = scanZones();
		scanAllRecordsByZone(zones);
		purgeOldData();
	}
	
	private void scanAllRecordsByZone(List<String> zones) {
		
		Instant startTime = Instant.now();

		logger.info("Scanning all records in each available zones");

		for(String zone: zones) {
			logger.debug("Scanning all records in Dynect zone {} ", zone);

			ObjectNode response = getDynectClient().get("REST/AllRecord/"+ zone);
			
			JsonNode recordsData = response.path("data");
			
			if(recordsData.isArray()) {
				recordsData.forEach(record ->{
					String recordFullName = record.asText();
					
					logger.debug("Scanning {} record for more information in {} zone", recordFullName, zone);
					ObjectNode recordResponse = getDynectClient().get(recordFullName);
					
					ObjectNode data = toRecordJson(recordResponse.get("data"));
					String recordId = Splitter.on("/").splitToList(recordFullName).get(5);
					
					String cypher = "MATCH (z:DynHostedZone {zoneName:{zone}})"
							+ "MERGE (m:DynHostedZoneRecord {recordId:{recordId}}) "
		    				+ "ON CREATE SET  m+={props}, m.createTs = timestamp(), m.updateTs=timestamp() "
		    				+ "ON MATCH SET m+={props}, m.updateTs=timestamp() "
		    				+ "MERGE (z)-[:CONTAINS]->(m);";
		    		getProjector().getNeoRxClient().execCypher(cypher, "recordId", recordId, "props", data, "zone", zone); 	
				});
			}	
		}
		
		Instant endTime = Instant.now();
		logger.info("Updating neo4j with the latest information of all records from zones in Dynect took {} secs", Duration.between(startTime, endTime).getSeconds());
	}

	private List<String> scanZones() {
		
		Instant startTime = Instant.now();

		logger.info("Scanning all zones in Dynect");
		ObjectNode response = getDynectClient().get("REST/Zone/");
		
        JsonNode zoneData = response.path("data");
        List<String> zonesList = new ArrayList<String>();
        
        
        if (zoneData.isArray()) {
        	zoneData.forEach(zone->{
        		
        		String zoneNode = zone.asText();
        		String zoneName = Splitter.on("/").splitToList(zoneNode).get(3);
        		zonesList.add(zoneName);   		
        	});
        }
        
        logger.info("Scanning {} zones in Dynect to get more details", zonesList.size() );

        
        for(String zone: zonesList) {
        	response = getDynectClient().get("REST/Zone/" + zone);
        	logger.debug("Scanning {} zone", zone);
        	
        	ObjectNode n = toZoneJson(response.get("data"));
    		Preconditions.checkNotNull(getProjector().getNeoRxClient(), "neorx client must be set");
    		String cypher = "MERGE (m:DynHostedZone {zoneName:{zone}}) "
    				+ "ON CREATE SET  m+={props}, m.createTs = timestamp(), m.updateTs=timestamp() "
    				+ "ON MATCH SET m+={props}, m.updateTs=timestamp();";
    		getProjector().getNeoRxClient().execCypher(cypher, "zone", zone, "props", n); 	
        }
        
		Instant endTime = Instant.now();
		logger.info("Updating neo4j with the latest information of all {} zones from Dynect took {} secs", zonesList.size(), 
				Duration.between(startTime, endTime).getSeconds());
     
        return zonesList;
	}
	
	private void purgeOldData() {

		logger.info("Purging old data of Dynect");
		long cutoff = System.currentTimeMillis() - (24 * 60 * 60 * 1000);
		getProjector().getNeoRxClient().execCypher("match ( x:DynHostedZone ) where x.updateTs<{cutoff} detach delete x", "cutoff", cutoff);
		getProjector().getNeoRxClient().execCypher("match ( x:DynHostedZoneRecord ) where x.updateTs<{cutoff} detach delete x", "cutoff", cutoff);
	}
	
	ObjectNode toRecordJson(JsonNode data) {
		ObjectNode n = mapper.createObjectNode();
		n.put("zoneName", data.get("zone").asText());
		n.put("ttl", data.get("ttl").asText());
		n.put("recordName", data.get("fqdn").asText());
		n.put("recordType", data.get("record_type").asText());
		
		JsonNode rData = data.get("rdata");
		
		Iterator<String> i = rData.fieldNames();
		
		while(i.hasNext()) {
			String key = i.next();			
			n.put(key, rData.get(key).asText());
		}
		
		return n;
	}

	ObjectNode toZoneJson(JsonNode data) {
		ObjectNode n = mapper.createObjectNode();
		n.put("zoneType", data.get("zone_type").asText());
		n.put("serialStyle", data.get("serial_style").asText());
		n.put("serial", data.get("serial").asText());
		return n;
	}

	class DynectClientSupplier implements Supplier<DynectClient> {

		@Override
		public DynectClient get() {
			DynectScannerBuilder builder = (DynectScannerBuilder) getBuilder();
			return new DynectClient.DynBuilder().withProperties(builder.getCompanyName(), builder.getUsername(), builder.getPassword()).build();
		}
	}

}
