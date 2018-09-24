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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.lendingclub.neorx.NeoRxClient;

import com.amazonaws.services.elasticloadbalancingv2.model.DescribeTargetGroupsRequest;
import com.amazonaws.services.elasticloadbalancingv2.model.DescribeTargetGroupsResult;
import com.amazonaws.services.elasticloadbalancingv2.model.DescribeTargetHealthRequest;
import com.amazonaws.services.elasticloadbalancingv2.model.DescribeTargetHealthResult;
import com.amazonaws.services.elasticloadbalancingv2.model.TargetGroup;
import com.amazonaws.services.elasticloadbalancingv2.model.TargetHealthDescription;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Strings;

public class TargetGroupScanner extends AbstractELBV2Scanner {

	public TargetGroupScanner(AWSScannerBuilder builder) {
		super(builder, "AwsElbTargetGroup");
	}

	@Override
	public Optional<String> computeArn(JsonNode n) {
		return Optional.of(n.path("aws_targetGroupArn").asText());
	}

	@Override
	protected void doScan() {
		DescribeTargetGroupsRequest request = new DescribeTargetGroupsRequest();
		GraphNodeGarbageCollector gc = newGarbageCollector().bindScannerContext();
		do {
			DescribeTargetGroupsResult result = getClient().describeTargetGroups(request);
			List<String> arns = new ArrayList<>();
			for (TargetGroup targetGroup : result.getTargetGroups()) {
				processTargetGroup(gc, targetGroup);
				arns.add(targetGroup.getTargetGroupArn());
				incrementEntityCount();
			}
			writeTagsToNeo4j(arns);
			request.setMarker(result.getNextMarker());
		} while (!Strings.isNullOrEmpty(request.getMarker()));
	}

	private void processTargetGroup(GraphNodeGarbageCollector gc, TargetGroup targetGroup) {
		ObjectNode n = convertAwsObject(targetGroup, getRegion());
		NeoRxClient neo4j = getNeoRxClient();
		neo4j.execCypher(
				"merge (n:AwsTargetGroup { aws_arn: {arn} }) set n += {props}, n.updateTs = timestamp() return n",
				"arn", targetGroup.getTargetGroupArn(), "props", n).forEach(gc.MERGE_ACTION);
		for (String loadBalancerArn : targetGroup.getLoadBalancerArns()) {
			neo4j.execCypher(
					"match (n:AwsTargetGroup { aws_arn: {arn} }), (m:AwsElbv2 { aws_arn: {elb} }) merge (m)-[:ROUTES_TO]->(n)",
					"arn", targetGroup.getTargetGroupArn(), "elb", loadBalancerArn);
		}
		DescribeTargetHealthResult healthResult = getClient().describeTargetHealth(
				new DescribeTargetHealthRequest().withTargetGroupArn(targetGroup.getTargetGroupArn()));

		long now = System.currentTimeMillis();
		for (TargetHealthDescription healthDescription : healthResult.getTargetHealthDescriptions()) {
			if (healthDescription.getTarget().getId().startsWith("i-")) {
				ObjectNode h = jsonConverter.toJson(healthDescription);
				String cypher = "match (n:AwsTargetGroup { aws_arn: {arn} }), "
						+ "(m:AwsEc2Instance { aws_region: {region}, aws_instanceId: {instance} })"
						+ " merge (n)-[r:TARGETS]->(m) set r += {props}, r.updateTs = {now}";
				neo4j.execCypher(cypher, "arn", targetGroup.getTargetGroupArn(), "region", getRegion().getName(),
						"instance", healthDescription.getTarget().getId(), "now", now, "props", h);
			}
		}
		neo4j.execCypher("match (n:AwsTargetGroup { aws_arn: {arn} })-[r:TARGETS]->(m:AwsEc2Instance)"
				+ " where r.updateTs < {now} delete r", "arn", targetGroup.getTargetGroupArn(), "now", now);
	}

}
