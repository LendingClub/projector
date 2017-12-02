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

import com.amazonaws.regions.Region;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient;
import com.fasterxml.jackson.databind.node.ObjectNode;

public abstract class IAMScanner extends GlobalAWSScanner<AmazonIdentityManagementClient> implements AWSSlowScan {

	public IAMScanner(AWSScannerBuilder builder,  String label) {
		super(builder, AmazonIdentityManagementClient.class, label);
		jsonConverter.flattenNestedObjects = true;
	}
	
	public String createIamArn(String entityType, String entityIdentifier) {
		return createArn("iam", null, entityType, entityIdentifier);
	}

	@Override
	public ObjectNode convertAwsObject(Object x, Region region) {
		return super.convertAwsObject(x, null);
	}

	protected void linkToAccount(String arn) {
		String cypher = "match (n:" + getNeo4jLabel() + " { aws_arn: {arn} }), (a:AwsAccount { aws_account: {a} })"
				+ " merge (n)-[r:OWNED_BY]->(a) set r.updateTs = timestamp()";
		getNeoRxClient().execCypher(cypher, "arn", arn, "a", getAccountId());
	}

}
