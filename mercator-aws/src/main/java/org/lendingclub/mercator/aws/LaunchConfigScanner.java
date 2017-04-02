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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import org.lendingclub.mercator.core.Projector;

import com.amazonaws.AmazonWebServiceClient;
import com.amazonaws.regions.Region;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClient;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClientBuilder;
import com.amazonaws.services.autoscaling.model.DescribeLaunchConfigurationsRequest;
import com.amazonaws.services.autoscaling.model.DescribeLaunchConfigurationsResult;
import com.amazonaws.services.autoscaling.model.LaunchConfiguration;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

public class LaunchConfigScanner extends AWSScanner<AmazonAutoScalingClient> {

	public LaunchConfigScanner(AWSScannerBuilder builder) {
		super(builder, AmazonAutoScalingClient.class,"AwsLaunchConfig");
	}

	@Override
	protected AmazonAutoScalingClient createClient() {
		return (AmazonAutoScalingClient) builder.configure(AmazonAutoScalingClientBuilder.standard()).build();

	}

	@Override
	public Optional<String> computeArn(JsonNode n) {
		return Optional.of(n.path("aws_launchConfigurationARN").asText());
	}

	@Override
	protected void doScan() {
		GraphNodeGarbageCollector gc = newGarbageCollector().bindScannerContext();

		forEachLaunchConfig(getRegion(), config -> {
			try {
				ObjectNode n = convertAwsObject(config, getRegion());
				List<String> securityGroups = getSecurityGroups(config);

				String cypher = "merge (x:AwsLaunchConfig {aws_arn:{aws_arn}}) set x+={props}, x.aws_securityGroups={sg}, x.updateTs=timestamp() return x";

				Preconditions.checkNotNull(getNeoRxClient());

				getNeoRxClient()
						.execCypher(cypher, "aws_arn", n.path("aws_arn").asText(), "props", n, "sg", securityGroups)
						.forEach(r -> {
							gc.MERGE_ACTION.accept(r);
							getShadowAttributeRemover().removeTagAttributes("AwsLaunchConfig", n, r);
						});
				incrementEntityCount();
			} catch (RuntimeException e) {
				maybeThrow(e);
			}
		});

	}

	private void forEachLaunchConfig(Region region, Consumer<LaunchConfiguration> consumer) {

		DescribeLaunchConfigurationsResult results = getClient()
				.describeLaunchConfigurations(new DescribeLaunchConfigurationsRequest());
		String token = results.getNextToken();
		results.getLaunchConfigurations().forEach(consumer);

		while (tokenHasNext(token)) {
			results = getClient()
					.describeLaunchConfigurations(new DescribeLaunchConfigurationsRequest().withNextToken(token));
			token = results.getNextToken();
			results.getLaunchConfigurations().forEach(consumer);
		}
	}

	protected List<String> getSecurityGroups(LaunchConfiguration c) {
		List<String> toReturnList = new ArrayList<>();
		JsonNode n = new ObjectMapper().valueToTree(c);

		JsonNode securityGroups = n.path("securityGroups");
		for (JsonNode sg : securityGroups) {
			toReturnList.add(sg.asText());
		}

		return toReturnList;
	}

}
