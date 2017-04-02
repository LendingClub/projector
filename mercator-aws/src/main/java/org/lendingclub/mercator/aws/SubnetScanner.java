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

import com.amazonaws.services.ec2.model.DescribeSubnetsResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;

public class SubnetScanner extends AbstractEC2Scanner{



	public SubnetScanner(AWSScannerBuilder builder) {
		super(builder);

	}



	@Override
	public Optional<String> computeArn(JsonNode n){
		
		String region = n.get(AWSScanner.AWS_REGION_ATTRIBUTE).asText();
		
		return Optional.of(String.format("arn:aws:ec2:%s:%s:subnet/%s",region,n.get(AccountScanner.ACCOUNT_ATTRIBUTE).asText(),n.get("aws_subnetId").asText()));
		
	}
	


	@Override
	protected void doScan() {
	
		DescribeSubnetsResult result = getClient().describeSubnets();

		GraphNodeGarbageCollector gc = newGarbageCollector().label("AwsSubnet").region(getRegion());
		
		result.getSubnets().forEach(it -> {
			try {
				ObjectNode n = convertAwsObject(it, getRegion());
				
				
				String cypher = "MERGE (v:AwsSubnet {aws_arn:{aws_arn}}) set v+={props}, v.updateTs=timestamp() return v";
				
				NeoRxClient client = getNeoRxClient();
				Preconditions.checkNotNull(client);
				client.execCypher(cypher, "aws_arn",n.get("aws_arn").asText(),"props",n).forEach(r->{
					gc.MERGE_ACTION.accept(r);
					getShadowAttributeRemover().removeTagAttributes("AwsSubnet", n, r);
				});
				incrementEntityCount();

			} catch (RuntimeException e) {
				gc.markException(e);
				maybeThrow(e,"problem scanning subnets");
			}
		});
		
		gc.invoke();
	}

	

}
