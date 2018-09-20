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

import java.util.Optional;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;

public class SQSScanner extends AWSScanner<AmazonSQSClient> {

	public SQSScanner(AWSScannerBuilder builder) {
		super(builder, AmazonSQSClient.class,"AwsSqsQueue");

	}

	@Override
	protected AmazonSQSClient createClient() {
		return createClient(AmazonSQSClient.class);
	}

	@Override
	protected void doScan() {

		ListQueuesResult result = getClient().listQueues();

		for (String url : result.getQueueUrls()) {
			try {
				scanQueue(url);
			} catch (RuntimeException e) {
				maybeThrow(e);
			}
		}
	}

	private void scanQueue(String url) {
		rateLimit();
		GetQueueAttributesResult result = getClient().getQueueAttributes(url, Lists.newArrayList("All"));
		
		ObjectNode n = convertAwsObject(result.getAttributes(), getRegion());
		n.put("url", url);
		
		mergeQueue(n);
	}

	@Override
	public Optional<String> computeArn(JsonNode n) {
		return Optional.ofNullable(n.path("aws_queueArn").asText(null));
	}

	private void mergeQueue(ObjectNode n) {

		incrementEntityCount();

		String cypher = "merge (t:AwsSqsQueue {aws_arn:{aws_arn}}) set t+={props}, t.updateTs=timestamp() return t";

		getNeoRxClient().execCypher(cypher, "aws_arn", n.path("aws_arn").asText(), "props", n).forEach(r -> {
			getShadowAttributeRemover().removeTagAttributes("AwsSqsQueue", n, r);
		});

		cypher = "match (a:AwsAccount {aws_account:{account}}), (q:AwsSqsQueue {aws_account:{account}}) MERGE (a)-[r:OWNS]->(q) set r.updateTs=timestamp()";

		getNeoRxClient().execCypher(cypher, "account", getAccountId());

	}

}
