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

import org.lendingclub.neorx.NeoRxClient;

import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient;
import com.amazonaws.services.identitymanagement.model.ListUsersRequest;
import com.amazonaws.services.identitymanagement.model.ListUsersResult;
import com.amazonaws.services.identitymanagement.model.User;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class UserScanner extends IAMScanner {

	public UserScanner(AWSScannerBuilder builder) {
		super(builder, "AwsIamUser");
	}

	@Override
	protected void doGlobalScan() {
		AmazonIdentityManagementClient iam = getClient();
		GraphNodeGarbageCollector gc = newGarbageCollector().bindScannerContext();
		NeoRxClient neo4j = getNeoRxClient();
		ListUsersResult result = iam.listUsers();
		while (true) {
			
			for (User user : result.getUsers()) {
				try {
					ObjectNode n = convertAwsObject(user, null);

					String cypher = "merge (n:AwsIamUser { aws_arn: {a} }) set n += {p}, n.updateTs = timestamp() return n";
					neo4j.execCypher(cypher, "a", user.getArn(), "p", n).forEach(it -> {
						gc.MERGE_ACTION.accept(it);
					});
					incrementEntityCount();
					linkToAccount(user.getArn());
				} catch (RuntimeException e) {
					gc.markException(e);
					maybeThrow(e);
				}
			}
			if (result.isTruncated()) {
				result = iam.listUsers(new ListUsersRequest().withMarker(result.getMarker()));
			} else {
				break;
			}
		}
	}
}
