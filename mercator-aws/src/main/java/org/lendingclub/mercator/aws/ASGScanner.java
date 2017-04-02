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
import java.util.function.Consumer;

import com.amazonaws.services.autoscaling.AmazonAutoScalingClient;
import com.amazonaws.services.autoscaling.model.AutoScalingGroup;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsRequest;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

public class ASGScanner extends AWSScanner<AmazonAutoScalingClient> {

	public ASGScanner(AWSScannerBuilder builder) {
		super(builder, AmazonAutoScalingClient.class,"AwsAsg");
	}

	@Override
	public Optional<String> computeArn(JsonNode n) {
		return Optional.of(n.path("aws_autoScalingGroupARN").asText());
	}

	public void scanASGNames(String... asgNames) {
		if (asgNames == null || asgNames.length == 0) {
			doScan();
		} else {
			doScan(asgNames);
		}
	}

	@Override
	protected void doScan() {
		doScan(new String[0]);
	}

	private void doScan(String... asgNames) {
		
		GraphNodeGarbageCollector gc = newGarbageCollector();
		if (asgNames==null || asgNames.length==0) {
			gc.bindScannerContext();
		}

		forEachAsg(asg -> {
			try {
				ObjectNode n = convertAwsObject(asg, getRegion());
				String asgArn = n.path("aws_arn").asText();

				String cypher = "merge (x:AwsAsg {aws_arn:{aws_arn}}) set x+={props}, x.updateTs=timestamp() return x";

				Preconditions.checkNotNull(getNeoRxClient());
				getNeoRxClient().execCypher(cypher, "aws_arn", asgArn, "props", n).forEach(r -> {
					gc.MERGE_ACTION.accept(r);
					getShadowAttributeRemover().removeTagAttributes("AwsAsg", n, r);
				});
				incrementEntityCount();
				mapAsgRelationships(asg, asgArn, getRegion().getName());
			} catch (RuntimeException e) {
		
				maybeThrow(e, "problem scanning asg");
			}

		}, asgNames);
		
	}

	private void forEachAsg(Consumer<AutoScalingGroup> consumer, String... asgNames) {

		DescribeAutoScalingGroupsRequest request = new DescribeAutoScalingGroupsRequest();
		if (asgNames != null && asgNames.length > 0) {
			request.withAutoScalingGroupNames(asgNames);
		}
		String token = null;
		do {
			DescribeAutoScalingGroupsResult results = getClient().describeAutoScalingGroups(request);
			token = results.getNextToken();
			results.getAutoScalingGroups().forEach(consumer);

			request.setNextToken(token);

		} while (tokenHasNext(token));
	}

	protected void mapAsgRelationships(AutoScalingGroup asg, String asgArn, String region) {
		JsonNode n = mapper.valueToTree(asg);

		String subnets = n.path("vpczoneIdentifier").asText().trim();
		String launchConfig = n.path("launchConfigurationName").asText().trim();
		JsonNode instances = n.path("instances");
		JsonNode elbs = n.path("loadBalancerNames");

		mapAsgToSubnet(subnets, asgArn, region);
		mapAsgToLaunchConfig(launchConfig, asgArn, region);
		mapAsgToInstance(instances, asgArn, region);
		mapAsgToElb(elbs, asgArn, region);
	}

	protected void mapAsgToLaunchConfig(String launchConfig, String asgArn, String region) {
		long updateTs = System.currentTimeMillis();
		String cypher = "match (x:AwsAsg {aws_arn:{asgArn}}), (y:AwsLaunchConfig {aws_launchConfigurationName:{lcn}, aws_region:{region}, aws_account:{account}}) "
				+ "merge (x)-[r:HAS]->(y) set r.updateTs={updateTs}";
		getNeoRxClient().execCypher(cypher, "asgArn", asgArn, "lcn", launchConfig, "region", region, "account",
				getAccountId(), "updateTs", updateTs);
		deleteObsoleteRelationships("AwsLaunchConfig", "HAS", asgArn, updateTs);
	}

	protected void mapAsgToSubnet(String subnets, String asgArn, String region) {
		long updateTs = System.currentTimeMillis();
		String[] arr = subnets.split(",");
		for (String s : arr) {
			String subnetArn = String.format("arn:aws:ec2:%s:%s:subnet/%s", region, getAccountId(), s.trim());

			String cypher = "match (x:AwsAsg {aws_arn:{asgArn}}), (y:AwsSubnet {aws_arn:{subnetArn}}) "
					+ "merge (x)-[r:LAUNCHES_INSTANCES_IN]->(y) set r.updateTs={updateTs}";
			getNeoRxClient().execCypher(cypher, "asgArn", asgArn, "subnetArn", subnetArn, "updateTs", updateTs);
		}
		deleteObsoleteRelationships("AwsSubnet", "LAUNCHES_INSTANCES_IN", asgArn, updateTs);
	}

	protected void mapAsgToInstance(JsonNode instances, String asgArn, String region) {
		long updateTs = System.currentTimeMillis();
		for (JsonNode i : instances) {
			String instanceId = i.path("instanceId").asText();
			String instanceArn = String.format("arn:aws:ec2:%s:%s:instance/%s", region, getAccountId(), instanceId);

			String cypher = "match (x:AwsEc2Instance {aws_arn:{instanceArn}}), (y:AwsAsg {aws_arn:{asgArn}}) "
					+ "merge (y)-[r:CONTAINS]->(x) set r.updateTs={updateTs}";
			getNeoRxClient().execCypher(cypher, "instanceArn", instanceArn, "asgArn", asgArn, "updateTs", updateTs);
		}
		deleteObsoleteRelationships("AwsEc2Instance", "CONTAINS", asgArn, updateTs);
	}

	protected void mapAsgToElb(JsonNode elbs, String asgArn, String region) {
		long updateTs = System.currentTimeMillis();
		for (JsonNode e : elbs) {
			String elbName = e.asText();
			String elbArn = String.format("arn:aws:elasticloadbalancing:%s:%s:loadbalancer/%s", region, getAccountId(),
					elbName);

			String cypher = "match (x:AwsElb {aws_arn:{elbArn}}), (y:AwsAsg {aws_arn:{asgArn}}) "
					+ "merge (y)-[r:ATTACHED_TO]-(x) set r.updateTs={updateTs}";
			getNeoRxClient().execCypher(cypher, "elbArn", elbArn, "asgArn", asgArn, "updateTs", updateTs);
		}
		deleteObsoleteRelationships("AwsElb", "ATTACHED_TO", asgArn, updateTs);
	}

	protected void deleteObsoleteRelationships(String targetLabel, String relationLabel, String asgArn, long updateTs) {
		// remove relationships not updated
		getNeoRxClient().execCypher("match (y:AwsAsg {aws_arn:{asgArn}})-[r:" + relationLabel + "]->(x:" + targetLabel
				+ ") " + " where r.updateTs < {updateTs} delete r", "asgArn", asgArn, "updateTs", updateTs);
	}

}
