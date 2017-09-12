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

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class KinesisScanner extends AWSScanner<AmazonKinesisClient> {

	public KinesisScanner(AWSScannerBuilder builder) {
		super(builder,AmazonKinesisClient.class,"AwsKinesisStream");
	}

	@Override
	protected AmazonKinesisClient createClient() {
		return (AmazonKinesisClient) builder.configure(AmazonKinesisClientBuilder
				.standard()).build();
	}

	@Override
	protected void doScan() {

		ListStreamsResult result = getClient().listStreams();
		
		for(String name: result.getStreamNames()) {
			try {
				scanStream(name);
			}
			catch (RuntimeException e) {
				maybeThrow(e,"problem scanning kinesis");
			}
		}

		
	}

	private void scanStream(String name) {
		rateLimit();
		com.amazonaws.services.kinesis.model.DescribeStreamResult result = getClient().describeStream(name);
		StreamDescription description = result.getStreamDescription();
		
		project(description);
		
	}
	private void project(StreamDescription description) {

		
		ObjectNode n = mapper.createObjectNode();
		n.put("aws_account", getAccountId());
		n.put("aws_region", getRegion().getName());
		n.put("aws_arn",description.getStreamARN());
		n.put("name",description.getStreamName());
		n.put("aws_name", description.getStreamName());
		n.put("aws_status", description.getStreamStatus());
		n.put("aws_streamCreationTimestamp",description.getStreamCreationTimestamp().getTime());
		n.put("aws_retentionPeriodHours",description.getRetentionPeriodHours());
		n.put("aws_shardCount", description.getShards().size());
		
		incrementEntityCount();
		String cypher = "merge (k:AwsKinesisStream {aws_arn:{aws_arn}}) set k+={props}, k.updateTs=timestamp() return k";

		getNeoRxClient().execCypher(cypher, "aws_arn", n.path("aws_arn").asText(), "props", n);

		cypher = "match (a:AwsAccount {aws_account:{account}}), (k:AwsKinesisStream {aws_account:{account}}) MERGE (a)-[r:OWNS]->(k) set r.updateTs=timestamp()";

		getNeoRxClient().execCypher(cypher, "account", getAccountId());

	}

	@Override
	public Optional<Double> getDefaultRateLimitPerSecond() {
		// kinesis describe operations are heavily rate limited on the AWS side
		return Optional.of(1d);
	}


}
