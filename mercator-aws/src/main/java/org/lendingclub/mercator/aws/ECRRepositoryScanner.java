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

import java.util.concurrent.TimeUnit;

import org.lendingclub.neorx.NeoRxClient;

import com.amazonaws.services.ecr.AmazonECRClient;
import com.amazonaws.services.ecr.model.DescribeRepositoriesRequest;
import com.amazonaws.services.ecr.model.DescribeRepositoriesResult;
import com.amazonaws.services.ecr.model.GetLifecyclePolicyRequest;
import com.amazonaws.services.ecr.model.GetRepositoryPolicyRequest;
import com.amazonaws.services.ecr.model.LifecyclePolicyNotFoundException;
import com.amazonaws.services.ecr.model.Repository;
import com.amazonaws.services.ecr.model.RepositoryPolicyNotFoundException;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ECRRepositoryScanner extends AWSScanner<AmazonECRClient> implements AWSSlowScan {

	public ECRRepositoryScanner(AWSScannerBuilder builder) {
		super(builder, AmazonECRClient.class, "AwsEcrRepository");
	}

	@Override
	protected void doScan() {
		AmazonECRClient ecr = getClient();
		GraphNodeGarbageCollector gc = newGarbageCollector().bindScannerContext();
		NeoRxClient neo4j = getNeoRxClient();
		DescribeRepositoriesRequest request = new DescribeRepositoriesRequest();
		do {
			DescribeRepositoriesResult result = ecr.describeRepositories(request);
			for (Repository repository : result.getRepositories()) {
				ObjectNode n = convertAwsObject(repository, getRegion());
				n.put("aws_arn", repository.getRepositoryArn());
				try {
					String policy = ecr
							.getRepositoryPolicy(
									new GetRepositoryPolicyRequest().withRepositoryName(repository.getRepositoryName()))
							.getPolicyText();
					n.put("aws_repositoryPolicy", policy);
				} catch (RepositoryPolicyNotFoundException e) {
				}
				try {
					String lifecyclePolicy = ecr
							.getLifecyclePolicy(
									new GetLifecyclePolicyRequest().withRepositoryName(repository.getRepositoryName()))
							.getLifecyclePolicyText();
					n.put("aws_lifecyclePolicy", lifecyclePolicy);
				} catch (LifecyclePolicyNotFoundException e) {
				}
				String cypher = "merge (n:AwsEcrRepository { aws_arn: {a} }) set n = {p}, n.updateTs = timestamp() return n";
				neo4j.execCypher(cypher, "a", repository.getRepositoryArn(), "p", n).forEach(gc.MERGE_ACTION);
			}
			request.setNextToken(result.getNextToken());
		} while (request.getNextToken() != null);
	}

	@Override
	public long getMinimumScanInterval() {
		return TimeUnit.MINUTES.toMillis(30L);
	}

}
