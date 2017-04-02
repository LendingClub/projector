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

import com.amazonaws.services.ec2.model.DescribeSecurityGroupsResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Strings;

public class SecurityGroupScanner extends AbstractEC2Scanner {

	public SecurityGroupScanner(AWSScannerBuilder builder) {
		super(builder);
		setNeo4jLabel("AwsSecurityGroup");
	}

	@Override
	public Optional<String> computeArn(JsonNode n) {

		// arn:aws:ec2:region:account-id:security-group/security-group-id
		return Optional.of(String.format("arn:aws:ec2:%s:%s:security-group/%s", n.get("aws_region").asText(),
				n.get("aws_account").asText(), n.get("aws_groupId").asText()));

	}

	@Override
	protected void doScan() {

		DescribeSecurityGroupsResult result = getClient().describeSecurityGroups();

		long now = System.currentTimeMillis();
		GraphNodeGarbageCollector gc = newGarbageCollector().bindScannerContext();
		result.getSecurityGroups().forEach(sg -> {

			try {
				
				ObjectNode g = convertAwsObject(sg, getRegion());

				// non-VPC security groups don't have a VPC
				String vpcId = Strings.nullToEmpty(sg.getVpcId());
				String cypher = "merge (sg:AwsSecurityGroup {aws_arn:{arn}}) set sg+={props}, sg.updateTs={now} return sg";

				JsonNode xx = getNeoRxClient()
						.execCypher(cypher, "arn", g.path(AWS_ARN_ATTRIBUTE).asText(), "props", g, "now", now).blockingFirst();
				getShadowAttributeRemover().removeTagAttributes("AwsSecurityGroup", g, xx);
				gc.updateEarliestTimestamp(xx);
				if (!vpcId.isEmpty()) {
					cypher = "match (v:AwsVpc {aws_vpcId: {vpcId}}), (sg:AwsSecurityGroup {aws_arn:{sg_arn}}) merge (sg)-[:RESIDES_IN]->(v)";
					getNeoRxClient().execCypher(cypher, "vpcId", vpcId, "sg_arn", g.path("aws_arn").asText());
				}
				incrementEntityCount();
			} catch (RuntimeException e) {
				maybeThrow(e, "problem scanning security groups");
			}
		});


	}

}
