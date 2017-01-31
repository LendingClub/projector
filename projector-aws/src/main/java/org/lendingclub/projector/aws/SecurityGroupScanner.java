/**
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
package org.lendingclub.projector.aws;

import java.util.Optional;

import com.amazonaws.services.ec2.model.DescribeSecurityGroupsResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Strings;

public class SecurityGroupScanner extends AbstractEC2Scanner {


	public SecurityGroupScanner(AWSScannerBuilder builder) {
		super(builder);

	}

	@Override
	public Optional<String> computeArn(JsonNode n) {
		return Optional.empty();
	}

	@Override
	public void doScan() {

		DescribeSecurityGroupsResult result = getClient().describeSecurityGroups();

		long now = System.currentTimeMillis();
		GraphNodeGarbageCollector gc = newGarbageCollector().region(getRegion()).label("AwsSecurityGroup");
		result.getSecurityGroups().forEach(sg -> {

			ObjectNode g = convertAwsObject(sg, getRegion());

			// non-VPC security groups don't have a VPC
			String vpcId = Strings.nullToEmpty(sg.getVpcId());
			String cypher = "merge (sg:AwsSecurityGroup {aws_account: {a}, aws_region: {r}, aws_vpcId: {vpcId}, aws_groupId: {groupId}}) set sg+={props}, sg.updateTs={now} return sg";

			JsonNode xx = getNeoRxClient().execCypher(cypher, "vpcId", vpcId, "groupId", sg.getGroupId(), "props", g,
					"now", now, "a", getAccountId(), "r", getRegion().getName()).toBlocking().first();

			gc.updateEarliestTimestamp(xx);
			if (!vpcId.isEmpty()) {
				cypher = "match (v:AwsVpc {aws_vpcId: {vpcId}}), (sg:AwsSecurityGroup {aws_groupId:{groupId}, aws_vpcId: {vpcId}}) merge (sg)-[:RESIDES_IN]->(v)";
				getNeoRxClient().execCypher(cypher, "vpcId", vpcId, "groupId", sg.getGroupId());
			}
		});

		gc.invoke();

	}

}
