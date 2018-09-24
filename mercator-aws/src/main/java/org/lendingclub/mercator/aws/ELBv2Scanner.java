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
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.amazonaws.services.elasticloadbalancingv2.model.AvailabilityZone;
import com.amazonaws.services.elasticloadbalancingv2.model.DescribeLoadBalancersRequest;
import com.amazonaws.services.elasticloadbalancingv2.model.DescribeLoadBalancersResult;
import com.amazonaws.services.elasticloadbalancingv2.model.LoadBalancer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;

public class ELBv2Scanner extends AbstractELBV2Scanner {

	public ELBv2Scanner(AWSScannerBuilder builder) {
		super(builder, "AwsElbv2");
	}

	public void scanLoadBalancerNames(String... loadBalancerNames) {
		if (loadBalancerNames == null || loadBalancerNames.length == 0) {
			return;
		}
		DescribeLoadBalancersRequest request = new DescribeLoadBalancersRequest();

		request.setNames(Arrays.asList(loadBalancerNames));

		String marker = null;
		do {
			rateLimit();
			DescribeLoadBalancersResult results = getClient().describeLoadBalancers(request);

			marker = results.getNextMarker();
			List<String> arns = new ArrayList<>();
			results.getLoadBalancers().forEach(it -> {
				projectElb(it, null);
				arns.add(it.getLoadBalancerArn());
			});
			writeTagsToNeo4j(arns);
			request.setMarker(marker);
		} while (tokenHasNext(marker));
	}

	@Override
	public Optional<String> computeArn(JsonNode n) {
		return Optional.of(n.path("aws_loadBalancerArn").asText());
	}

	private void projectElb(LoadBalancer elb, GraphNodeGarbageCollector gc) {
		ObjectNode n = convertAwsObject(elb, getRegion());
		incrementEntityCount();

		logger.debug("Scanning elb: {}", elb.getLoadBalancerArn());

		String cypher = "merge (x:AwsElbv2 {aws_arn:{aws_arn}}) set x+={props} set x.updateTs=timestamp() return x";

		Preconditions.checkNotNull(getNeoRxClient());

		getNeoRxClient().execCypher(cypher, "aws_arn", elb.getLoadBalancerArn(), "props", n).forEach(it -> {
			if (gc != null) {
				gc.MERGE_ACTION.accept(it);
			}

		});

		mapElbRelationships(elb);
	}

	@Override
	protected void doScan() {

		GraphNodeGarbageCollector gc = newGarbageCollector().bindScannerContext();

		DescribeLoadBalancersRequest request = new DescribeLoadBalancersRequest();

		String marker = null;
		do {
			rateLimit();
			DescribeLoadBalancersResult results = getClient().describeLoadBalancers(request.withMarker(marker));
			marker = results.getNextMarker();
			List<String> arns = new ArrayList<>();
			for (LoadBalancer elb : results.getLoadBalancers()) {
				incrementEntityCount();
				projectElb(elb, gc);
				arns.add(elb.getLoadBalancerArn());
			}
			writeTagsToNeo4j(arns);
			request.setMarker(marker);
		} while (tokenHasNext(marker));
	}

	protected void mapElbRelationships(LoadBalancer lb) {
		mapElbToSubnet(lb);
		mapElbToSecurityGroups(lb);
	}

	protected void mapElbToSecurityGroups(LoadBalancer lb) {
		if (lb.getSecurityGroups() == null) {
			// network load balancers don't have security groups
			return;
		}
		LinkageHelper linkage = newLinkageHelper().withFromArn(lb.getLoadBalancerArn())
				.withTargetLabel("AwsSecurityGroup").withLinkLabel("ATTACHED_TO")
				.withTargetValues(lb.getSecurityGroups().stream().map(sg -> createArn("ec2", "security-group", sg))
						.collect(Collectors.toList()));
		linkage.execute();
	}

	protected void mapElbToSubnet(LoadBalancer lb) {
		for (AvailabilityZone az : lb.getAvailabilityZones()) {
			String cypher = "match (x:AwsElbv2 {aws_arn:{elbArn}}),"
					+ " (y:AwsSubnet { aws_region: {region}, aws_subnetId: {subnetId} }) "
					+ "merge (x)-[r:AVAILABLE_IN]->(y) set r.updateTs=timestamp()";
			getNeoRxClient().execCypher(cypher, "elbArn", lb.getLoadBalancerArn(), "region", getRegion().getName(),
					"subnetId", az.getSubnetId());
		}
	}

	@Override
	public Optional<Double> getDefaultRateLimitPerSecond() {
		return Optional.of(2d);
	}

}
