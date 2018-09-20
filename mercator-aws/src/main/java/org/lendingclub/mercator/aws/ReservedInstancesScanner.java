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
import java.util.concurrent.TimeUnit;

import org.lendingclub.neorx.NeoRxClient;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeReservedInstancesResult;
import com.amazonaws.services.ec2.model.RecurringCharge;
import com.amazonaws.services.ec2.model.ReservedInstances;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ReservedInstancesScanner extends AbstractEC2Scanner implements AWSSlowScan {

	public ReservedInstancesScanner(AWSScannerBuilder builder) {
		super(builder, "AwsReservedInstances");
		jsonConverter.withFlattenNestedObjects(true);
	}

	@Override
	public Optional<String> computeArn(JsonNode n) {
		return Optional.of(createEc2Arn("reserved-instances", n.path("aws_reservedInstancesId").asText()));
	}

	@Override
	protected void doScan() {
		AmazonEC2Client ec2 = getClient();
		GraphNodeGarbageCollector gc = newGarbageCollector().bindScannerContext();
		DescribeReservedInstancesResult response = ec2.describeReservedInstances();
		for (ReservedInstances reservedInstance : response.getReservedInstances()) {
			ObjectNode n = convertAwsObject(reservedInstance, getRegion());
			for (RecurringCharge recurringCharge : reservedInstance.getRecurringCharges()) {
				n.put("aws_recurringCharge_" + recurringCharge.getFrequency().toLowerCase(),
						recurringCharge.getAmount());
			}
			NeoRxClient neo4j = getNeoRxClient();
			String cypher = "merge (n:AwsReservedInstances { aws_arn: {a} }) set n += {p}, n.updateTs = timestamp() return n";
			neo4j.execCypher(cypher, "a", createEc2Arn("reserved-instances", reservedInstance.getReservedInstancesId()),
					"p", n).forEach(it -> {
						gc.MERGE_ACTION.accept(it);
					});
			incrementEntityCount();
		}

	}

	@Override
	public long getMinimumScanInterval() {
		return TimeUnit.MINUTES.toMillis(61L);
	}

}
