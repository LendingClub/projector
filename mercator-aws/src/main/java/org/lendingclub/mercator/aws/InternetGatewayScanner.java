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
package org.lendingclub.mercator.aws;

import java.util.Optional;

import org.lendingclub.neorx.NeoRxClient;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.InternetGateway;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class InternetGatewayScanner extends AbstractEC2NetworkInfrastructureScanner {

	public InternetGatewayScanner(AWSScannerBuilder builder) {
		super(builder, "AwsInternetGateway");
	}

	@Override
	protected void doScan() {
		AmazonEC2Client ec2 = getClient();
		GraphNodeGarbageCollector gc = newGarbageCollector().bindScannerContext();
		ec2.describeInternetGateways().getInternetGateways().forEach(c -> scanInternetGateway(gc, c));
	}

	@Override
	public Optional<String> computeArn(JsonNode n) {
		return Optional.of(createEc2Arn("internet-gateway", n.path("aws_internetGatewayId").asText()));
	}

	private void scanInternetGateway(GraphNodeGarbageCollector gc, InternetGateway c) {
		NeoRxClient neo4j = getNeoRxClient();
		ObjectNode n = convertAwsObject(c, getRegion());
		neo4j.execCypher("merge (n:AwsInternetGateway { aws_arn: {a} }) set n += {p} return n", "a", n.path("aws_arn"), "p",
				n).forEach(gc.MERGE_ACTION);
		incrementEntityCount();
	}

}
