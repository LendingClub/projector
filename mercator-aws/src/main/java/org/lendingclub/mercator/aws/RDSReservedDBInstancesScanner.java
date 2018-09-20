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

import com.amazonaws.services.rds.AmazonRDSClient;
import com.amazonaws.services.rds.model.DescribeReservedDBInstancesRequest;
import com.amazonaws.services.rds.model.DescribeReservedDBInstancesResult;
import com.amazonaws.services.rds.model.ReservedDBInstance;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Strings;

public class RDSReservedDBInstancesScanner extends AWSScanner<AmazonRDSClient> implements AWSSlowScan {

	public RDSReservedDBInstancesScanner(AWSScannerBuilder builder) {
		super(builder, AmazonRDSClient.class, "AwsRdsReservedDbInstances");
		jsonConverter.withFlattenNestedObjects(true);
	}

	@Override
	protected void doScan() {
		AmazonRDSClient rds = getClient();
		DescribeReservedDBInstancesRequest request = new DescribeReservedDBInstancesRequest();
		GraphNodeGarbageCollector gc = newGarbageCollector().bindScannerContext();
		do {
			DescribeReservedDBInstancesResult result = rds.describeReservedDBInstances(request);
			for (ReservedDBInstance reservedInstances : result.getReservedDBInstances()) {
				processReservedInstances(gc, reservedInstances);
			}
			request.setMarker(result.getMarker());
		} while (!Strings.isNullOrEmpty(request.getMarker()));
	}

	private void processReservedInstances(GraphNodeGarbageCollector gc, ReservedDBInstance reservedInstances) {
		NeoRxClient neo4j = getNeoRxClient();
		ObjectNode n = convertAwsObject(reservedInstances, getRegion());
		neo4j.execCypher(
				"merge (n:AwsRdsReservedDbInstances { aws_arn: {arn} }) set n += {props}, n.updateTs = timestamp() return n",
				"arn", reservedInstances.getReservedDBInstanceArn(), "props", n).forEach(r -> {
					gc.MERGE_ACTION.accept(r);
					getShadowAttributeRemover().removeTagAttributes(getNeo4jLabel(), n, r);
				});
		incrementEntityCount();
	}

	@Override
	public long getMinimumScanInterval() {
		return TimeUnit.HOURS.toMillis(1L);
	}

}
