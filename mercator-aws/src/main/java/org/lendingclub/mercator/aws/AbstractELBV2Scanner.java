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

import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancingClient;
import com.amazonaws.services.elasticloadbalancingv2.model.DescribeTagsRequest;
import com.amazonaws.services.elasticloadbalancingv2.model.DescribeTagsResult;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;

public abstract class AbstractELBV2Scanner extends AWSScanner<AmazonElasticLoadBalancingClient> {

	static final int DESCRIBE_TAGS_MAX = 20;

	public AbstractELBV2Scanner(AWSScannerBuilder builder, String label) {
		super(builder, AmazonElasticLoadBalancingClient.class, label);
		jsonConverter.flattenNestedObjects = true;
	}

	protected void writeTagsToNeo4j(List<String> resourceArns) {

		// DescribeTags takes at most 20 names at a time
		for (int i = 0; i < resourceArns.size(); i += DESCRIBE_TAGS_MAX) {
			try {
				List<String> subsetArns = resourceArns.subList(i, Math.min(i + DESCRIBE_TAGS_MAX, resourceArns.size()));
				rateLimit();

				DescribeTagsResult describeTagsResult = getClient()
						.describeTags(new DescribeTagsRequest().withResourceArns(subsetArns));
				describeTagsResult.getTagDescriptions().forEach(tag -> {
					try {

						ObjectNode n = jsonConverter.toJson(tag);
						n.remove("aws_resourceArn");

						String elbArn = tag.getResourceArn();

						String cypher = "merge (x:" + getNeo4jLabel() + " {aws_arn:{aws_arn}}) set x+={props} return x";

						Preconditions.checkNotNull(getNeoRxClient());

						getNeoRxClient().execCypher(cypher, "aws_arn", elbArn, "props", n).forEach(r -> {
							getShadowAttributeRemover().removeTagAttributes(getNeo4jLabel(), n, r);
						});

					} catch (RuntimeException e) {
						maybeThrow(e, "problem scanning ELB tags");

					}
				});
			} catch (RuntimeException e) {
				maybeThrow(e, "problem scanning ELB tags");
			}
		}
	}

}
