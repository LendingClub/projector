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
package org.lendingclub.mercator.aws;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.lendingclub.mercator.core.MercatorException;

import com.amazonaws.regions.Region;
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingClient;
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingClientBuilder;
import com.amazonaws.services.elasticloadbalancing.model.DescribeLoadBalancersRequest;
import com.amazonaws.services.elasticloadbalancing.model.DescribeLoadBalancersResult;
import com.amazonaws.services.elasticloadbalancing.model.DescribeTagsRequest;
import com.amazonaws.services.elasticloadbalancing.model.DescribeTagsResult;
import com.amazonaws.services.elasticloadbalancing.model.LoadBalancerDescription;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

public class ELBScanner extends AWSScanner<AmazonElasticLoadBalancingClient> {
	private static final int DESCRIBE_TAGS_MAX = 20;

	public ELBScanner(AWSScannerBuilder builder) {
		super(builder, AmazonElasticLoadBalancingClient.class,"AwsElb");

	}

	@Override
	protected AmazonElasticLoadBalancingClient createClient() {
		return (AmazonElasticLoadBalancingClient) builder.configure(AmazonElasticLoadBalancingClientBuilder.standard())
				.build();
	}

	public void scanLoadBalancerNames(String... loadBalancerNames) {
		if (loadBalancerNames == null || loadBalancerNames.length == 0) {
			return;
		}
		DescribeLoadBalancersRequest request = new DescribeLoadBalancersRequest();

		request.setLoadBalancerNames(Arrays.asList(loadBalancerNames));

		String marker = null;
		do {
			DescribeLoadBalancersResult results = getClient().describeLoadBalancers(request);

			marker = results.getNextMarker();
			results.getLoadBalancerDescriptions().forEach(it -> {
				projectElb(it, null);
				
			});
			writeTagsToNeo4j(results, getRegion(), getClient());
			request.setMarker(marker);
		} while (tokenHasNext(marker));
	}

	@Override
	public Optional<String> computeArn(JsonNode n) {
		return Optional.ofNullable(ArnGenerator
				.newInstance(n.path(AccountScanner.ACCOUNT_ATTRIBUTE).asText(),
						n.path(AWSScanner.AWS_REGION_ATTRIBUTE).asText())
				.createElbArn(n.path("aws_loadBalancerName").asText()));

	}

	private void projectElb(LoadBalancerDescription elb, GraphNodeGarbageCollector gc) {
		ObjectNode n = convertAwsObject(elb, getRegion());
		incrementEntityCount();
		String elbArn = n.path("aws_arn").asText();
		logger.debug("Scanning elb: {}", elbArn);

		String cypher = "merge (x:AwsElb {aws_arn:{aws_arn}}) set x+={props} set x.updateTs=timestamp() return x";

		Preconditions.checkNotNull(getNeoRxClient());

		getNeoRxClient().execCypher(cypher, "aws_arn", elbArn, "props", n).forEach(it -> {
			if (gc != null) {
				gc.MERGE_ACTION.accept(it);
			}
		
		});

		mapElbRelationships(elb, elbArn, getRegion().getName());
	}

	@Override
	protected void doScan() {

		GraphNodeGarbageCollector gc = newGarbageCollector().bindScannerContext();

		forEachElb(getRegion(), elb -> {
			try {
				incrementEntityCount();
				projectElb(elb, gc);

			} catch (RuntimeException e) {
	
				maybeThrow(e,"problem scanning ELB");
				
			}
		});


	}

	private void forEachElb(Region region, Consumer<LoadBalancerDescription> consumer) {

		DescribeLoadBalancersRequest request = new DescribeLoadBalancersRequest();

		
		String marker = null;
		do {

			DescribeLoadBalancersResult results = getClient().describeLoadBalancers(request.withMarker(marker));
			marker = results.getNextMarker();
			results.getLoadBalancerDescriptions().forEach(consumer);
			writeTagsToNeo4j(results, region, getClient());
			request.setMarker(marker);
		} while  (tokenHasNext(marker));
	}

	protected void writeTagsToNeo4j(DescribeLoadBalancersResult results, Region region,
			AmazonElasticLoadBalancingClient client) {
		if (!results.getLoadBalancerDescriptions().isEmpty()) {

			List<String> loadBalancerNames = results.getLoadBalancerDescriptions().stream()
					.map(lb -> lb.getLoadBalancerName()).collect(Collectors.toList());

			// DescribeTags takes at most 20 names at a time
			for (int i = 0; i < loadBalancerNames.size(); i += DESCRIBE_TAGS_MAX) {
				List<String> subsetNames = loadBalancerNames.subList(i,
						Math.min(i + DESCRIBE_TAGS_MAX, loadBalancerNames.size()));
				DescribeTagsResult describeTagsResult = client
						.describeTags(new DescribeTagsRequest().withLoadBalancerNames(subsetNames));
				describeTagsResult.getTagDescriptions().forEach(tag -> {
					try {
						ObjectNode n = convertAwsObject(tag, region);
						
						String elbArn = n.path("aws_arn").asText();

						String cypher = "merge (x:AwsElb {aws_arn:{aws_arn}}) set x+={props} return x";

						Preconditions.checkNotNull(getNeoRxClient());

						getNeoRxClient().execCypher(cypher, "aws_arn", elbArn, "props", n).forEach(r -> {
							getShadowAttributeRemover().removeTagAttributes("AwsElb", n, r);
						});
						
					} catch (RuntimeException e) {
						maybeThrow(e,"problem scanning ELB tags");
						
					}
				});
			}
		}
	}

	protected void mapElbRelationships(LoadBalancerDescription lb, String elbArn, String region) {
		JsonNode n = mapper.valueToTree(lb);
		JsonNode subnets = n.path("subnets");
		JsonNode instances = n.path("instances");
		JsonNode securityGroups = n.path("securityGroups");

		mapElbToSubnet(subnets, elbArn, region);
		mapElbToInstance(instances, elbArn, region);
		addSecurityGroups(securityGroups, elbArn);

	}

	protected void addSecurityGroups(JsonNode securityGroups, String elbArn) {
		List<String> l = new ArrayList<>();
		for (JsonNode s : securityGroups) {
			l.add(s.asText());
		}

		String cypher = "match (x:AwsElb {aws_arn:{aws_arn}}) set x.aws_securityGroups={sg}";
		getNeoRxClient().execCypher(cypher, "aws_arn", elbArn, "sg", l);
	}

	protected void mapElbToSubnet(JsonNode subnets, String elbArn, String region) {

		for (JsonNode s : subnets) {
			String subnetName = s.asText();
			String subnetArn = String.format("arn:aws:ec2:%s:%s:subnet/%s", region, getAccountId(), subnetName);
			String cypher = "match (x:AwsElb {aws_arn:{elbArn}}), (y:AwsSubnet {aws_arn:{subnetArn}}) "
					+ "merge (x)-[r:AVAILABLE_IN]->(y) set r.updateTs=timestamp()";
			getNeoRxClient().execCypher(cypher, "elbArn", elbArn, "subnetArn", subnetArn);
		}
	}

	protected void mapElbToInstance(JsonNode instances, String elbArn, String region) {

		AtomicLong oldestRelationshipTs = new AtomicLong(Long.MAX_VALUE);
		for (JsonNode i : instances) {

			String instanceName = i.path("instanceId").asText();
			String instanceArn = String.format("arn:aws:ec2:%s:%s:instance/%s", region, getAccountId(), instanceName);
			// logger.info("{} instanceArn: {}",elbArn,instanceArn);
			String cypher = "match (x:AwsElb {aws_arn:{elbArn}}), (y:AwsEc2Instance {aws_arn:{instanceArn}}) "
					+ "merge (x)-[r:DISTRIBUTES_TRAFFIC_TO]->(y) set r.updateTs=timestamp() return x,r,y";
			getNeoRxClient().execCypher(cypher, "elbArn", elbArn, "instanceArn", instanceArn).forEach(r -> {
				oldestRelationshipTs.set(Math.min(r.path("r").path("updateTs").asLong(), oldestRelationshipTs.get()));
			});

			if (oldestRelationshipTs.get() > 0 && oldestRelationshipTs.get() < Long.MAX_VALUE) {
				cypher = "match (x:AwsElb {aws_arn:{elbArn}})-[r:DISTRIBUTES_TRAFFIC_TO]-(y:AwsEc2Instance) where r.updateTs<{oldest}  delete r";
				getNeoRxClient().execCypher(cypher, "elbArn", elbArn, "oldest", oldestRelationshipTs.get());
			}
		}
	}
}
