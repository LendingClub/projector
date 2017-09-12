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

import java.util.List;

import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.ListSubscriptionsByTopicResult;
import com.amazonaws.services.sns.model.ListTopicsResult;
import com.amazonaws.services.sns.model.Subscription;
import com.amazonaws.services.sns.model.Topic;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;

public class SNSScanner extends AWSScanner<AmazonSNSClient> {

	public SNSScanner(AWSScannerBuilder builder) {
		super(builder, AmazonSNSClient.class, "AwsSnsTopic");

	}

	@Override
	protected AmazonSNSClient createClient() {
		return (AmazonSNSClient) builder.configure(AmazonSNSClientBuilder.standard()).build();
	}

	@Override
	protected void doScan() {

		rateLimit();
		ListTopicsResult result = getClient().listTopics();
		String token = null;
		do {
			token = result.getNextToken();
			for (Topic topic : result.getTopics()) {
				try {
					projectTopic(topic);
					scanSubscriptions(topic);
				} catch (RuntimeException e) {
					maybeThrow(e);
				}
			}
			result = getClient().listTopics(token);
		} while (tokenHasNext(token));

	}

	private void projectTopic(Topic topic) {

		String arn = topic.getTopicArn();

		List<String> parts = Splitter.on(":").splitToList(arn);
		incrementEntityCount();
		ObjectNode n = mapper.createObjectNode();
		n.put("aws_account", getAccountId());
		n.put("aws_region", getRegion().getName());
		n.put("name", parts.get(parts.size() - 1));
		String cypher = "merge (t:AwsSnsTopic {aws_arn:{arn}}) set t+={props}, t.updateTs=timestamp() return t";

		getNeoRxClient().execCypher(cypher, "arn", arn, "props", n).forEach(r -> {
			getShadowAttributeRemover().removeTagAttributes("AwsSnsTopic", n, r);
		});
		cypher = "match (a:AwsAccount {aws_account:{account}}), (t:AwsSnsTopic {aws_account:{account}}) MERGE (a)-[r:OWNS]->(t) set r.updateTs=timestamp()";

		getNeoRxClient().execCypher(cypher, "account", getAccountId());

	}

	private void scanSubscriptions(Topic topic) {

		ListSubscriptionsByTopicResult result = getClient().listSubscriptionsByTopic(topic.getTopicArn());
		String token = null;
		do {
			rateLimit();
			token = result.getNextToken();
			for (Subscription subscription : result.getSubscriptions()) {
				projectSubscription(topic, subscription);

			}
			result = getClient().listSubscriptionsByTopic(topic.getTopicArn(), token);

		} while ((!Strings.isNullOrEmpty(token)) && (!token.equals("null")));
	}

	private void projectSubscription(Topic topic, Subscription subscription) {

		ObjectNode n = mapper.createObjectNode();
		n.put("aws_topicArn", subscription.getTopicArn());
		n.put("aws_endpoint", subscription.getEndpoint());
		n.put("aws_protocol", subscription.getProtocol());
		n.put("aws_owner", subscription.getOwner());
		n.put("aws_region", getRegion().getName());
		n.put("aws_account", getAccountId());
		String cypher = "merge (s:AwsSnsSubscription {aws_arn:{arn}}) set s+={props}, s.updateTs=timestamp()";

		getNeoRxClient().execCypher(cypher, "arn", subscription.getSubscriptionArn(), "props", n);

		cypher = "match (a:AwsSnsSubscription {aws_arn:{subscriptionArn}}), (t:AwsSnsTopic {aws_arn:{topicArn}}) MERGE (t)-[r:HAS_SUBSCRIPTION]->(a) set r.updateTs=timestamp()";

		getNeoRxClient().execCypher(cypher, "subscriptionArn", subscription.getSubscriptionArn(), "topicArn",
				topic.getTopicArn());

		String targetArn = subscription.getEndpoint();
		if (targetArn.startsWith("arn:aws:sqs:")) {
			cypher = "match (a:AwsSnsTopic {aws_arn:{topicArn}}),(q:AwsSqsQueue {aws_arn:{queueArn}}) MERGE (a)-[r:PUBLISHES]->(q) set r.updateTs=timestamp()";
			getNeoRxClient().execCypher(cypher, "topicArn", subscription.getTopicArn(), "queueArn", targetArn);

		}
	}

}
