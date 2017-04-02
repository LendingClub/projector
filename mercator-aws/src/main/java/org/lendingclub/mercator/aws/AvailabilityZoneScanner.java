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
package org.lendingclub.mercator.aws;

import java.util.Optional;

import org.lendingclub.neorx.NeoRxClient;

import com.amazonaws.services.ec2.model.DescribeAvailabilityZonesResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;

public class AvailabilityZoneScanner extends AbstractEC2Scanner{





	public AvailabilityZoneScanner(AWSScannerBuilder builder) {
		super(builder);
		setNeo4jLabel("AwsAvailabilityZone");
	}

	@Override
	public Optional<String> computeArn(JsonNode n) {
		return Optional.empty();
	}

	@Override
	protected void doScan() {
		

		GraphNodeGarbageCollector gc = newGarbageCollector().bindScannerContext();
		DescribeAvailabilityZonesResult result = getClient().describeAvailabilityZones();
		result.getAvailabilityZones().forEach(it -> {
			
			try {
		
				ObjectNode n = convertAwsObject(it, getRegion());
				
				String cypher = "merge (y:AwsAvailabilityZone {aws_zoneName:{aws_zoneName}, aws_region:{aws_region}, aws_account:{aws_account}}) set y+={props} set y.updateTs=timestamp() return y";
				String mapToSubnetCypher = "match (x:AwsSubnet {aws_availabilityZone:{aws_zoneName}, aws_region:{aws_region}, aws_account:{aws_account}}), "
						+ "(y:AwsAvailabilityZone {aws_zoneName:{aws_zoneName}, aws_region:{aws_region}, aws_account:{aws_account}}) "
						+ "merge (x)-[r:RESIDES_IN]->(y) set r.updateTs=timestamp()";
				String mapToRegionCypher = "match (x:AwsRegion {aws_regionName:{aws_region}, aws_account:{aws_account}}), "
						+ "(y:AwsAvailabilityZone {aws_zoneName:{aws_zoneName}, aws_regionName:{aws_region}, aws_account:{aws_account}}) "
						+ "merge (x)-[r:CONTAINS]->(y) set r.updateTs=timestamp()";
				
				NeoRxClient neoRx = getNeoRxClient();	
				Preconditions.checkNotNull(neoRx);
			
				neoRx.execCypher(cypher, "aws_zoneName",n.path("aws_zoneName").asText(), AWSScanner.AWS_REGION_ATTRIBUTE,n.path(AWSScanner.AWS_REGION_ATTRIBUTE).asText(), AccountScanner.ACCOUNT_ATTRIBUTE,getAccountId(), "props",n).forEach(gc.MERGE_ACTION);
				neoRx.execCypher(mapToSubnetCypher, "aws_zoneName",n.path("aws_zoneName").asText(), AWSScanner.AWS_REGION_ATTRIBUTE,n.path(AWSScanner.AWS_REGION_ATTRIBUTE).asText(), AccountScanner.ACCOUNT_ATTRIBUTE,getAccountId());
				neoRx.execCypher(mapToRegionCypher, "aws_zoneName",n.path("aws_zoneName").asText(), AWSScanner.AWS_REGION_ATTRIBUTE,n.path(AWSScanner.AWS_REGION_ATTRIBUTE).asText(),AccountScanner. ACCOUNT_ATTRIBUTE,getAccountId());

			} catch (RuntimeException e) { 
				maybeThrow(e,"problem scanning availability zones");
			}		
		});		

	}

}
