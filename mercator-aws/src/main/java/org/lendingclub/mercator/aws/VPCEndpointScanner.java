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

import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;

import org.lendingclub.neorx.NeoRxClient;

import com.amazonaws.services.ec2.model.DescribeVpcEndpointsRequest;
import com.amazonaws.services.ec2.model.DescribeVpcEndpointsResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Strings;

public class VPCEndpointScanner extends AbstractEC2NetworkInfrastructureScanner {
	public VPCEndpointScanner(AWSScannerBuilder builder) {
		super(builder, "AwsVpcEndpoint");
		jsonConverter.withFlattenNestedObjects(true);
	}

	@Override
	protected void doScan() {
		GraphNodeGarbageCollector gc = newGarbageCollector().bindScannerContext();
		DescribeVpcEndpointsRequest request = new DescribeVpcEndpointsRequest();
		do {
			DescribeVpcEndpointsResult result = getClient().describeVpcEndpoints(request);
			result.getVpcEndpoints().forEach(endpoint -> {
				ObjectNode n = convertAwsObject(endpoint, getRegion());

				NeoRxClient neo4j = getNeoRxClient();
				try {
					String cypher = "merge (x:AwsVpcEndpoint {aws_arn:{arn}}) set x+={props}, x.updateTs=timestamp() return x";
					String arn = n.path("aws_arn").asText();
					neo4j.execCypher(cypher, "arn", arn, "props", n).forEach(it -> {
						gc.MERGE_ACTION.accept(it);
					});

					LinkageHelper routeTableLinkage = newLinkageHelper().withFromArn(arn)
							.withTargetLabel("AwsRouteTable").withLinkLabel("AVAILABLE_IN")
							.withTargetValues(endpoint.getRouteTableIds().stream()
									.map(r -> createEc2Arn("route-table", r)).collect(Collectors.toList()));

					routeTableLinkage.execute();

					LinkageHelper vpcLinkage = newLinkageHelper().withFromArn(arn).withTargetLabel("AwsVpc")
							.withLinkLabel("OWNED_BY")
							.withTargetValues(Collections.singletonList(createEc2Arn("vpc", endpoint.getVpcId())));
					vpcLinkage.execute();

					incrementEntityCount();
				} catch (RuntimeException e) {
					gc.markException(e);
					maybeThrow(e);
				}
			});
			request.setNextToken(result.getNextToken());
		} while (!Strings.isNullOrEmpty(request.getNextToken()));
	}

	@Override
	public Optional<String> computeArn(JsonNode n) {
		/*
		 * The arn format for vpc endpoints isn't actually documented, but let's assume
		 * it's true.
		 * 
		 * http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.
		 * html#arn-syntax-ec2
		 */
		return Optional.of(createEc2Arn("vpc-endpoint", n.path("aws_vpcEndpointId").asText()));
	}

}
