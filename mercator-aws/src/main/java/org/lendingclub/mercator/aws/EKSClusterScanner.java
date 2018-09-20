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

import com.amazonaws.services.eks.AmazonEKSClient;
import com.amazonaws.services.eks.model.Cluster;
import com.amazonaws.services.eks.model.DescribeClusterRequest;
import com.amazonaws.services.eks.model.DescribeClusterResult;
import com.amazonaws.services.eks.model.ListClustersRequest;
import com.amazonaws.services.eks.model.ListClustersResult;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class EKSClusterScanner extends AWSScanner<AmazonEKSClient> implements AWSSlowScan {

	public EKSClusterScanner(AWSScannerBuilder builder) {
		super(builder, AmazonEKSClient.class, "AwsEKSCluster");
		jsonConverter.flattenNestedObjects = true;
	}

	@Override
	public long getMinimumScanInterval() {
		return TimeUnit.MINUTES.toMillis(20L);
	}

	@Override
	protected void doScan() {
		AmazonEKSClient eks = getClient();
		GraphNodeGarbageCollector gc = newGarbageCollector().bindScannerContext();
		ListClustersRequest request = new ListClustersRequest();
		do {
			ListClustersResult result = eks.listClusters(request);
			for (String clusterName : result.getClusters()) {
				DescribeClusterResult clusterResult = eks
						.describeCluster(new DescribeClusterRequest().withName(clusterName));
				updateCluster(gc, clusterResult.getCluster());
			}
			request.setNextToken(result.getNextToken());
		} while (request.getNextToken() != null);
	}

	private void updateCluster(GraphNodeGarbageCollector gc, Cluster cluster) {
		ObjectNode n = convertAwsObject(cluster, getRegion());
		n.remove("aws_certificateAuthority_data");
		getNeoRxClient().execCypher("merge (n:AwsEKSCluster { aws_arn: {arn} }) set n += {props} return n", "arn",
				cluster.getArn(), "props", n).forEach(gc.MERGE_ACTION);
		LinkageHelper linkageHelper = newLinkageHelper().withFromArn(cluster.getArn()).withMoreWhere(
				"b.aws_region = {region} and b.aws_account = {account}", "region", getRegion().getName(), "account",
				getAccountId());
		linkageHelper.copy().withLinkLabel("CONTROL_PLANE_EXISTS_IN").withTargetLabel("AwsSubnet")
				.withTargetLinkAttribute("aws_subnetId")
				.withTargetValues(cluster.getResourcesVpcConfig().getSubnetIds()).execute();
		linkageHelper.copy().withLinkLabel("CONTROL_PLANE_SECURED_BY").withTargetLabel("AwsSecurityGroup")
				.withTargetLinkAttribute("aws_groupId")
				.withTargetValues(cluster.getResourcesVpcConfig().getSecurityGroupIds()).execute();

	}

}
