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

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.EnhancedMetrics;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;

public class KinesisScanner extends AWSScanner<AmazonKinesisClient> implements AWSSlowScan {

	public KinesisScanner(AWSScannerBuilder builder) {
		super(builder, AmazonKinesisClient.class, "AwsKinesisStream");
		jsonConverter.flattenNestedObjects = true;
	}

	@Override
	protected AmazonKinesisClient createClient() {
		return (AmazonKinesisClient) builder.configure(AmazonKinesisClientBuilder.standard()).build();
	}

	@Override
	protected void doScan() {

		ListStreamsRequest request = new ListStreamsRequest();
		ListStreamsResult result;
		GraphNodeGarbageCollector gc = newGarbageCollector().bindScannerContext();
		do {
			result = getClient().listStreams(request);
			List<String> streamNames = result.getStreamNames();
			for (String name : streamNames) {
				try {
					scanStream(gc, name);
				} catch (RuntimeException e) {
					maybeThrow(e, "problem scanning kinesis");
				}
			}
			request.setExclusiveStartStreamName(
					result.isHasMoreStreams() ? streamNames.get(streamNames.size() - 1) : null);
		} while (!Strings.isNullOrEmpty(request.getExclusiveStartStreamName()));
	}

	private void scanStream(GraphNodeGarbageCollector gc, String name) {
		rateLimit();
		// TODO - handle hasMoreShards
		DescribeStreamResult result = AmazonServiceRetry.execute(() -> getClient().describeStream(name));
		StreamDescription description = result.getStreamDescription();
		project(gc, description);
	}

	private void project(GraphNodeGarbageCollector gc, StreamDescription description) {
		ObjectNode n = convertAwsObject(description, getRegion());
		n.put("aws_arn", description.getStreamARN());
		n.put("aws_shardCount", description.getShards().size());

		// the API here is silly
		Set<String> shardLevelMetrics = new TreeSet<>();
		for (EnhancedMetrics metrics : description.getEnhancedMonitoring()) {
			shardLevelMetrics.addAll(metrics.getShardLevelMetrics());
		}
		n.put("aws_shardLevelMetrics", Joiner.on(' ').join(shardLevelMetrics));

		incrementEntityCount();
		String cypher = "merge (k:AwsKinesisStream {aws_arn:{aws_arn}}) set k+={props}, k.updateTs=timestamp() return k";

		getNeoRxClient().execCypher(cypher, "aws_arn", n.path("aws_arn").asText(), "props", n).forEach(gc.MERGE_ACTION);

		cypher = "match (a:AwsAccount {aws_account:{account}}), (k:AwsKinesisStream {aws_account:{account}}) MERGE (a)-[r:OWNS]->(k) set r.updateTs=timestamp()";

		getNeoRxClient().execCypher(cypher, "account", getAccountId());

	}

	@Override
	public Optional<Double> getDefaultRateLimitPerSecond() {
		// kinesis describe operations are heavily rate limited on the AWS side
		return Optional.of(2.0);
	}

	@Override
	public long getMinimumScanInterval() {
		return TimeUnit.HOURS.toMillis(1L);
	}

}
