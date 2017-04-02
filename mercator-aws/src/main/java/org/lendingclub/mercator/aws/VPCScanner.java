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

import com.amazonaws.services.ec2.model.DescribeVpcsResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;

public class VPCScanner extends AbstractEC2Scanner {




	public VPCScanner(AWSScannerBuilder builder) {
		super(builder);

	}


	@Override
	protected void doScan() {

	
		DescribeVpcsResult result = getClient().describeVpcs();

		GraphNodeGarbageCollector gc = newGarbageCollector().region(getRegion()).label("AwsVpc");
		NeoRxClient neoRx = getNeoRxClient();
		Preconditions.checkNotNull(neoRx);
		
		result.getVpcs().forEach(it -> {
			try {					
				ObjectNode n = convertAwsObject(it, getRegion());
									
				String cypher = "merge (x:AwsVpc {aws_arn:{aws_arn}}) set x+={props} set x.updateTs=timestamp() return x";
				
				String mapToSubnetCypher = "match (y:AwsSubnet {aws_vpcId:{aws_vpcId}}), "
						+ "(x:AwsVpc {aws_arn:{aws_arn}}) "
						+ "merge (x)-[r:CONTAINS]->(y) set r.updateTs=timestamp()";
				
				neoRx.execCypher(cypher, "aws_arn",n.path("aws_arn").asText(), "props",n).forEach(r->{
					gc.MERGE_ACTION.accept(r);
					getShadowAttributeRemover().removeTagAttributes("AwsVpc", n, r);
				});
				neoRx.execCypher(mapToSubnetCypher, "aws_arn",n.path("aws_arn").asText(), "aws_vpcId",n.path("aws_vpcId").asText());	
				incrementEntityCount();
			} catch (RuntimeException e) { 
				maybeThrow(e);
			}
		});
	
		String mapAccountCypher = "match (x:AwsAccount {aws_account:{aws_account}}), (y:AwsVpc {aws_account:{aws_account}}) "
				+ "merge (x)-[r:OWNS]->(y) set r.updateTs=timestamp()";
		String mapRegionCypher = "match (x:AwsVpc {aws_region:{aws_region}}), (y:AwsRegion {aws_regionName:{aws_region}, aws_account:{aws_account}}) "
				+ "merge (x)-[r:RESIDES_IN]->(y) set r.updateTs=timestamp()";
		
		neoRx.execCypher(mapAccountCypher, AccountScanner.ACCOUNT_ATTRIBUTE,getAccountId());
		neoRx.execCypher(mapRegionCypher, AWSScanner.AWS_REGION_ATTRIBUTE, getRegion().getName(), AccountScanner.ACCOUNT_ATTRIBUTE,getAccountId());
		gc.invoke();
	}
	

	@Override
	public Optional<String> computeArn(JsonNode n) {

		String region = n.get(AWSScanner.AWS_REGION_ATTRIBUTE).asText();

		return Optional.of(String.format("arn:aws:ec2:%s:%s:vpc/%s", region, n.get(AccountScanner.ACCOUNT_ATTRIBUTE).asText(),
				n.get("aws_vpcId").asText()));
	}
}
