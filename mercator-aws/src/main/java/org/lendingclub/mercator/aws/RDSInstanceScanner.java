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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import org.lendingclub.neorx.NeoRxClient;

import com.amazonaws.services.rds.AmazonRDSClient;
import com.amazonaws.services.rds.AmazonRDSClientBuilder;
import com.amazonaws.services.rds.model.DBInstance;
import com.amazonaws.services.rds.model.DescribeDBInstancesResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

public class RDSInstanceScanner extends AWSScanner<AmazonRDSClient> {




	public RDSInstanceScanner(AWSScannerBuilder builder) {
		super(builder,AmazonRDSClient.class,"AwsRdsInstance");
	
	}

	@Override
	protected AmazonRDSClient createClient() {
		return (AmazonRDSClient) builder.configure(AmazonRDSClientBuilder
				.standard()).build();
	}

	@Override
	public Optional<String> computeArn(JsonNode n) {
		
		String region = n.get(AWSScanner.AWS_REGION_ATTRIBUTE).asText(null);
		String account = n.get(AccountScanner.ACCOUNT_ATTRIBUTE).asText(null);
		String dbInstanceId = n.get("aws_dbinstanceIdentifier").asText(null);
		
		Preconditions.checkState(!Strings.isNullOrEmpty(region), "aws_region not set");
		Preconditions.checkState(!Strings.isNullOrEmpty(account), "aws_account not set");
		Preconditions.checkState(!Strings.isNullOrEmpty(dbInstanceId), "aws_dbinstanceIdentifier not set");

		return Optional.of(String.format("arn:aws:rds:%s:%s:db:%s", region, account, dbInstanceId));
	}

	@Override
	protected void doScan() {
		
		GraphNodeGarbageCollector gc = newGarbageCollector().bindScannerContext();
		
		forEachInstance( instance -> {
			try { 
				ObjectNode n = convertAwsObject(instance, getRegion());
				NeoRxClient neoRx = getNeoRxClient();
				Preconditions.checkNotNull(neoRx);
				
				String rdsArn = n.path("aws_arn").asText();
				
				String cypher = "merge (x:AwsRdsInstance {aws_arn:{aws_arn}}) set x+={props} set x.updateTs=timestamp()";
				neoRx.execCypher(cypher, "aws_arn", rdsArn, "props",n).forEach(r->{
					gc.MERGE_ACTION.accept(r);
					getShadowAttributeRemover().removeTagAttributes("AwsRdsInstance", n, r);
				});
				
				List<String> subnets = getSubnets(instance);
				for (String s : subnets) { 
					String subnetArn = computeSubnetArn(s, n);
					
					String mapToSubnetCypher = "match (x:AwsRdsInstance {aws_arn:{rdsArn}}), "
							+ "(y:AwsSubnet {aws_arn:{subnetArn}}) "
							+ "merge (x)-[r:AVAILABLE_IN]->(y) set r.updateTs=timestamp()";
					neoRx.execCypher(mapToSubnetCypher, "rdsArn",rdsArn, "subnetArn",subnetArn);
				}
			} catch (RuntimeException e) { 
		
				maybeThrow(e,"problem scanning RDS Instance");
			}
		});
	
	} 
	
	private void forEachInstance(Consumer<DBInstance> consumer) { 

		DescribeDBInstancesResult result = getClient().describeDBInstances();
		String marker = result.getMarker();

		result.getDBInstances().forEach(consumer);
		
		while (tokenHasNext(marker)) { 
			result = getClient().describeDBInstances().withMarker(marker);
			marker = result.getMarker();
			result.getDBInstances().forEach(consumer);
		}
	}
	
	
	protected String computeSubnetArn(String subnetId, ObjectNode n) {
		String region = n.path(AWSScanner.AWS_REGION_ATTRIBUTE).asText(null);
		String account = n.path(AccountScanner.ACCOUNT_ATTRIBUTE).asText(null);
		
		Preconditions.checkState(!Strings.isNullOrEmpty(region), "aws_region must not be null");
		Preconditions.checkState(!Strings.isNullOrEmpty(account), "aws_account must not be null");

		String subnetArn = String.format("arn:aws:ec2:%s:%s:subnet/%s", region, account, subnetId);
		
		return subnetArn;
	}
	
	
	protected List<String> getSubnets(DBInstance i) { 
		List<String> listToReturn = new ArrayList<>();
		JsonNode n = new ObjectMapper().valueToTree(i);
		
		JsonNode subnets = n.path("dbsubnetGroup").path("subnets");
		for (JsonNode s : subnets) { 
			String subnetId = s.path("subnetIdentifier").asText();
			if (!Strings.isNullOrEmpty(subnetId)) { 
				listToReturn.add(subnetId);
			}
		}		
		
		return listToReturn;
	}

}
