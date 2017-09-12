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
package org.lendingclub.mercator.bind;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.lendingclub.mercator.bind.model.ResourceRecordSet;
import org.lendingclub.mercator.bind.model.ToDnsRecord;
import org.lendingclub.mercator.core.AbstractScanner;
import org.lendingclub.mercator.core.Scanner;
import org.lendingclub.mercator.core.ScannerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

public class BindScanner extends AbstractScanner {
	
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	private Supplier<BindClient> clientSupplier = Suppliers.memoize(new BindClientSupplier());
	
	public BindClient getBindClient() {
		return clientSupplier.get();
	}


	public BindScanner(ScannerBuilder<? extends Scanner> builder) {
		super(builder);
	}

	@Override
	public void scan() {
		
		scanZones();
		scanRecordSetByZone();
		purgeOldData();
	}

	@SuppressWarnings("unchecked")
	private void scanRecordSetByZone() {
		
		Instant startTime = Instant.now();
	
		List<String> zones = getBindClient().getZones();
		Preconditions.checkNotNull(getProjector().getNeoRxClient(), "neorx client must be set");

		zones.forEach(zone->{
				
				logger.info("Scanning the zone {}", zone);
				Optional<List> recordsStream = getBindClient().getRecordsbyZone(zone);
				
				if (!recordsStream.get().isEmpty()) {
					
					logger.info("Found {} records in {} zone", recordsStream.get().size(), zone);
					recordsStream.get().forEach( record -> {
						String line = record.toString();

						if(!line.startsWith(";")) {

		                    ResourceRecordSet<Map<String, Object>> dnsRecord = getRecord(line);
		                    
		    				ObjectNode recordNode = dnsRecord.toJson();
		    				String cypher = "MATCH (z:BindHostedZone {zoneName:{zone}}) "
		    						+ "MERGE (m:BindHostedZoneRecord {domainName:{dn}, type:{type}, zoneName:{zone}}) "
		    						+ "ON CREATE SET m.ttl={ttl}, m.class={class}, m+={props}, m.createTs = timestamp(), m.updateTs=timestamp() "
		    						+ "ON MATCH SET m+={props}, m.updateTs=timestamp() "
		    						+ "MERGE (z)-[:CONTAINS]->(m);";
		    				getProjector().getNeoRxClient().execCypher(cypher, "dn", recordNode.get("name").asText() , "type", recordNode.get("type").asText(), "ttl", 
		    						recordNode.get("ttl").asText(), "class", recordNode.get("class").asText(),  "props", recordNode.get("rData"), "zone", zone);

						}				
					});
				} else {
					logger.error("Failed to obtain any records in {} zone", zone);
				}		
		});
			
		Instant endTime = Instant.now();
		logger.info(" Took {} secs to project Bind records to Neo4j", Duration.between(startTime, endTime).getSeconds());
	}

	private void scanZones() {
		
		Instant startTime = Instant.now();

		List<String> zones = getBindClient().getZones();

		zones.forEach(zone -> {
			
			String cypher = "MERGE (m:BindHostedZone {zoneName:{zone}}) "
					+ "ON CREATE SET m.createTs = timestamp(), m.updateTs=timestamp() "
					+ "ON MATCH SET  m.updateTs=timestamp();";
			getProjector().getNeoRxClient().execCypher(cypher, "zone", zone);
		});
		Instant endTime = Instant.now();
		logger.info(" Took {} secs to project Bind Zone information to Neo4j", Duration.between(startTime, endTime).getSeconds());

	}
	
	protected ResourceRecordSet<Map<String, Object>> getRecord( String line) {
		String[] fields = line.split("\\s+");
		
		Map<String, Object> rData = ToDnsRecord.toRData(fields[3], fields);
        ResourceRecordSet<Map<String, Object>> dnsRecord = new ResourceRecordSet<Map<String, Object>>(fields[0], Integer.parseInt(fields[1]), fields[2], fields[3], rData);
        return dnsRecord;
	}
	
	private void purgeOldData() {

		logger.info("Purging old data of Bind");
		long cutoff = System.currentTimeMillis() - (24 * 60 * 60 * 1000);
		getProjector().getNeoRxClient().execCypher("match ( m:BindHostedZoneRecord ) where m.updateTs<{cutoff} detach delete m", "cutoff", cutoff);
	}
	
	class BindClientSupplier implements Supplier<BindClient> {

		@Override
		public BindClient get() {
			BindScannerBuilder builder = (BindScannerBuilder) getBuilder();
			return new BindClient.BindBuilder().withProperties(builder.getDnsServer(), builder.getZones(), builder.getKeyName(), builder.getKeyValue(), builder.getKeyAlgo()).build();
		}
	}
}
