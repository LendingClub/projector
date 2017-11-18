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

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.lendingclub.mercator.core.ScannerContext;
import org.lendingclub.neorx.NeoRxClient;

import com.amazonaws.regions.Region;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;

public class EC2InstanceScanner extends AbstractEC2Scanner {

	public EC2InstanceScanner(AWSScannerBuilder builder) {
		super(builder, "AwsEc2Instance");
		jsonConverter.flattenNestedObjects = true;
	}

	@Override
	public Optional<String> computeArn(JsonNode n) {

		String region = n.path(AWSScanner.AWS_REGION_ATTRIBUTE).asText(null);
		String account = n.path(AccountScanner.ACCOUNT_ATTRIBUTE).asText(null);
		String instanceId = n.path("aws_instanceId").asText(null);

		Preconditions.checkState(!Strings.isNullOrEmpty(region), "aws_region not set");
		Preconditions.checkState(!Strings.isNullOrEmpty(account), "aws_account not set");
		Preconditions.checkState(!Strings.isNullOrEmpty(instanceId), "aws_instanceId not set");

		return Optional.of(ArnGenerator.newInstance(account, region).createEc2InstanceArn(instanceId));

	}

	private void writeInstance(Instance instance) {
		writeInstance(instance, null);
	}

	private void writeInstance(Instance instance, GraphNodeGarbageCollector gc) {
		Stopwatch sw = Stopwatch.createStarted();
		Stopwatch instanceMergeStopwatch = Stopwatch.createUnstarted();
		Stopwatch amiMergeStopwatch = Stopwatch.createUnstarted();
		Stopwatch subnetMergeStopwatch = Stopwatch.createUnstarted();
		try {
			if (instance.getState().getName().equals("terminated")) {
				// instance is terminated
				// we may want to take the opportunity to delete it right here
			} else {
				JsonNode n = convertAwsObject(instance, getRegion());
				NeoRxClient neoRx = getNeoRxClient();

				String subnetId = n.path("aws_subnetId").asText(null);
				String instanceArn = n.path("aws_arn").asText(null);
				String account = n.path(AccountScanner.ACCOUNT_ATTRIBUTE).asText(null);
				String imageId = n.path("aws_imageId").asText(null);

				Preconditions.checkNotNull(neoRx);

				Preconditions.checkState(!Strings.isNullOrEmpty(instanceArn), "aws_arn must not be null");
				Preconditions.checkState(!Strings.isNullOrEmpty(account), "aws_account must not be null");

				instanceMergeStopwatch.start();
				String createInstanceCypher = "merge (x:AwsEc2Instance {aws_arn:{instanceArn}}) set x+={props}, x.updateTs=timestamp() return x";
				if (gc != null) {
					neoRx.execCypher(createInstanceCypher, "instanceArn", instanceArn, "props", n).forEach(it -> {
						gc.MERGE_ACTION.accept(it);
						shadowRemover.removeTagAttributes("AwsEc2Instance", n, it);
					});
				} else {
					neoRx.execCypher(createInstanceCypher, "instanceArn", instanceArn, "props", n);
				}
				instanceMergeStopwatch.stop();

				amiMergeStopwatch.start();
				if (!Strings.isNullOrEmpty(imageId)) {
					String amiArn = String.format("arn:aws:ec2:%s::image/%s", getRegion().getName(), imageId);

					String mapToImageCypher = "match (x:AwsAmi {aws_arn:{amiArn}}), "
							+ "(y:AwsEc2Instance {aws_arn:{instanceArn}}) "
							+ "merge (y)-[r:USES]-(x) set r.updateTs=timestamp()";
					neoRx.execCypher(mapToImageCypher, "amiArn", amiArn, "instanceArn", instanceArn);
				}
				amiMergeStopwatch.stop();

				subnetMergeStopwatch.start();
				if (!Strings.isNullOrEmpty(subnetId)) {
					String subnetArn = String.format("arn:aws:ec2:%s:%s:subnet/%s", getRegion().getName(), account,
							subnetId);
					String mapToSubnetCypher = "match (x:AwsSubnet {aws_arn:{subnetArn}}), "
							+ "(y:AwsEc2Instance {aws_arn:{instanceArn}}) "
							+ "merge (y)-[r:RESIDES_IN]->(x) set r.updateTs=timestamp()";
					neoRx.execCypher(mapToSubnetCypher, "subnetArn", subnetArn, "instanceArn", instanceArn);

				}
				subnetMergeStopwatch.stop();

				LinkageHelper securityGroupLinkage = newLinkageHelper().withFromArn(instanceArn)
						.withLinkLabel("ATTACHED_TO").withTargetLabel("AwsSecurityGroup")
						.withTargetValues(instance.getSecurityGroups().stream()
								.map(sg -> createEc2Arn("security-group", sg.getGroupId()))
								.collect(Collectors.toList()));
				securityGroupLinkage.execute();
			}
		} finally {
			if (sw.elapsed(TimeUnit.MILLISECONDS) > 50) {
				logger.info("writeInstance took {} ms (instance={}ms amiRelationship={}ms subnetRelationship={}ms)",
						sw.elapsed(TimeUnit.MILLISECONDS), instanceMergeStopwatch.elapsed(TimeUnit.MILLISECONDS),
						amiMergeStopwatch.elapsed(TimeUnit.MILLISECONDS),
						subnetMergeStopwatch.elapsed(TimeUnit.MILLISECONDS));
			}
		}
	}

	public void scanInstanceId(String... instanceIdList) {

		Arrays.asList(instanceIdList).forEach(instanceId -> {

			String token = null;
			DescribeInstancesRequest request = new DescribeInstancesRequest().withInstanceIds(instanceId);

			do {
				rateLimit();
				DescribeInstancesResult results = getClient().describeInstances();
				results.getReservations().forEach(reservation -> {
					reservation.getInstances().forEach(instance -> {
						try {
							writeInstance(instance);
						} catch (RuntimeException e) {
							maybeThrow(e);
						}
					});
				});
				request.setNextToken(results.getNextToken());
			} while (tokenHasNext(token));
		});
	}

	@Override
	protected void doScan() {
		GraphNodeGarbageCollector gc = new GraphNodeGarbageCollector().neo4j(getNeoRxClient()).account(getAccountId())
				.label("AwsEc2Instance").region(getRegion().getName()).bindScannerContext();

		forEachInstance(getRegion(), instance -> {

			try {

				writeInstance(instance, gc);
				ScannerContext.getScannerContext().get().incrementEntityCount();
			} catch (RuntimeException e) {
				gc.markException(e);
				maybeThrow(e);
			}

		});

	}

	private void forEachInstance(Region region, Consumer<Instance> consumer) {

		rateLimit();
		DescribeInstancesResult results = getClient().describeInstances(new DescribeInstancesRequest());

		String token = results.getNextToken();
		results.getReservations().forEach(reservation -> {
			rateLimit();
			reservation.getInstances().forEach(consumer);
		});

		while (!Strings.isNullOrEmpty(token) && !token.equals("null")) {
			rateLimit();
			results = getClient().describeInstances(new DescribeInstancesRequest().withNextToken(token));
			token = results.getNextToken();
			results.getReservations().forEach(reservation -> {
				reservation.getInstances().forEach(consumer);
			});
		}
	}

}
