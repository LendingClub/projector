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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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
	
	private List<String> targetLoadBalancerNames;



	public ELBScanner(AWSScannerBuilder builder) {
		super(builder);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected AmazonElasticLoadBalancingClient createClient() {
		return (AmazonElasticLoadBalancingClient) builder.configure(AmazonElasticLoadBalancingClientBuilder
				.standard()).build();
	}

	public ELBScanner withLoadBalancerNames(Collection<String> loadBalancerNames) {
		this.targetLoadBalancerNames = loadBalancerNames.isEmpty() ? null : new ArrayList<>(loadBalancerNames);
		return this;
	}

	public ELBScanner withLoadBalancerNames(String... loadBalancerNames) {
		return withLoadBalancerNames(Arrays.asList(loadBalancerNames));
	}

	@Override
	public Optional<String> computeArn(JsonNode n) {
		return Optional
				.of(String.format("arn:aws:elasticloadbalancing:%s:%s:loadbalancer/%s", n.path(AWSScanner.AWS_REGION_ATTRIBUTE).asText(),
						n.path(AccountScanner.ACCOUNT_ATTRIBUTE).asText(), n.path("aws_loadBalancerName").asText()));
	}

	@Override
	public void doScan() {

		GraphNodeGarbageCollector gc = newGarbageCollector().label("AwsElb").region(getRegion());
		
		forEachElb(getRegion(), elb -> {
			try {
				ObjectNode n = convertAwsObject(elb, getRegion());

				String elbArn = n.path("aws_arn").asText();

				String cypher = "merge (x:AwsElb {aws_arn:{aws_arn}}) set x+={props} set x.updateTs=timestamp() return x";

				Preconditions.checkNotNull(getNeoRxClient());

				getNeoRxClient().execCypher(cypher, "aws_arn", elbArn, "props", n).forEach(gc.MERGE_ACTION);

				mapElbRelationships(elb, elbArn, getRegion().getName());

			} catch (RuntimeException e) {
				logger.warn("problem scanning ELBs", e);
			}
		});
		if (targetLoadBalancerNames == null) {
			// gc only if we scan all load balancers
			gc.invoke();
		}
	}
	
	
	private void forEachElb(Region region, Consumer<LoadBalancerDescription> consumer) { 
		
		
		DescribeLoadBalancersRequest request = new DescribeLoadBalancersRequest();
		if (targetLoadBalancerNames != null) {
			request.setLoadBalancerNames(targetLoadBalancerNames);
		}
		
		DescribeLoadBalancersResult results = getClient().describeLoadBalancers(request);
		String marker = results.getNextMarker();
		
		results.getLoadBalancerDescriptions().forEach(consumer);
		writeTagsToNeo4j(results, region, getClient());
		
		while (!Strings.isNullOrEmpty(marker) && !marker.equals("null")) { 
			results = getClient().describeLoadBalancers(request.withMarker(marker));
			marker = results.getNextMarker();
			results.getLoadBalancerDescriptions().forEach(consumer);
			writeTagsToNeo4j(results, region, getClient());
		}
	}
	
	protected void writeTagsToNeo4j(DescribeLoadBalancersResult results, Region region, AmazonElasticLoadBalancingClient client) { 
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

						getNeoRx().execCypher(cypher, "aws_arn", elbArn, "props", n);
					} catch (RuntimeException e) {
						logger.warn("problem scanning ELB tags", e);
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
		getNeoRx().execCypher(cypher, "aws_arn", elbArn, "sg", l);
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

		for (JsonNode i : instances) {
			String instanceName = i.path("instanceId").asText();
			String instanceArn = String.format("arn:aws:ec2:%s:%s:instance/%s", region, getAccountId(), instanceName);
			String cypher = "match (x:AwsElb {aws_arn:{elbArn}}), (y:AwsEc2Instance {aws_arn:{instanceArn}}) "
					+ "merge (x)-[r:DISTRIBUTES_TRAFFIC_TO]->(y) set r.updateTs=timestamp()";
			getNeoRx().execCypher(cypher, "elbArn", elbArn, "instanceArn", instanceArn);

		}
	}
}
