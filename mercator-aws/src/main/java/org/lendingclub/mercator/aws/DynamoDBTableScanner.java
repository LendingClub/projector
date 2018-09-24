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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Strings;

public class DynamoDBTableScanner extends AWSScanner<AmazonDynamoDBClient> implements AWSSlowScan {

	public DynamoDBTableScanner(AWSScannerBuilder builder) {
		super(builder, AmazonDynamoDBClient.class, "AwsDynamoDBTable");
		jsonConverter.withFlattenNestedObjects(true);
	}

	@Override
	protected void doScan() {
		AmazonDynamoDBClient client = getClient();

		GraphNodeGarbageCollector gc = newGarbageCollector().bindScannerContext();

		ListTablesRequest request = new ListTablesRequest();
		do {
			ListTablesResult result = client.listTables(request);
			for (String tableName : result.getTableNames()) {
				scanTable(client, gc, tableName);
			}
			request.setExclusiveStartTableName(result.getLastEvaluatedTableName());
		} while (!Strings.isNullOrEmpty(request.getExclusiveStartTableName()));
	}

	private void scanTable(AmazonDynamoDBClient client, GraphNodeGarbageCollector gc, String tableName) {
		TableDescription table;

		try {
			table = client.describeTable(new DescribeTableRequest(tableName)).getTable();
		} catch (ResourceNotFoundException e) {
			return;
		}

		ObjectNode n = convertAwsObject(table, getRegion());

		getNeoRxClient().execCypher(
				"merge (n:AwsDynamoDBTable { aws_arn: {arn} }) set n += {props}, n.updateTs = timestamp() return n",
				"arn", table.getTableArn(), "props", n).forEach(gc.MERGE_ACTION);
		
		incrementEntityCount();
	}

	@Override
	public long getMinimumScanInterval() {
		return TimeUnit.MINUTES.toMillis(75L);
	}

	@Override
	public Optional<Double> getDefaultRateLimitPerSecond() {
		return Optional.of(0.5);
	}

}
