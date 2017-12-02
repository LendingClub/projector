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

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import org.lendingclub.neorx.NeoRxClient;

import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient;
import com.amazonaws.services.identitymanagement.model.GetRolePolicyRequest;
import com.amazonaws.services.identitymanagement.model.GetRolePolicyResult;
import com.amazonaws.services.identitymanagement.model.ListRolePoliciesRequest;
import com.amazonaws.services.identitymanagement.model.ListRolePoliciesResult;
import com.amazonaws.services.identitymanagement.model.ListRolesRequest;
import com.amazonaws.services.identitymanagement.model.ListRolesResult;
import com.amazonaws.services.identitymanagement.model.Role;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class RoleScanner extends IAMScanner {

	public RoleScanner(AWSScannerBuilder builder) {
		super(builder, "AwsIamRole");
	}

	@Override
	protected void doGlobalScan() {
		AmazonIdentityManagementClient iam = getClient();
		ListRolesResult result = iam.listRoles();

		GraphNodeGarbageCollector gc = newGarbageCollector().bindScannerContext();
		NeoRxClient neo4j = getNeoRxClient();
		while (true) {

			for (Role role : result.getRoles()) {
				try {
					ObjectNode n = convertAwsObject(role, null);
					n.put("aws_assumeRolePolicyDocument", urlDecode(role.getAssumeRolePolicyDocument()));

					String cypher = "merge (n:AwsIamRole { aws_arn: {a} }) set n += {p}, n.updateTs = timestamp() return n";
					neo4j.execCypher(cypher, "a", role.getArn(), "p", n).forEach(it -> {
						gc.MERGE_ACTION.accept(it);
					});
					incrementEntityCount();
					
					ListRolePoliciesResult rolePolicies = iam
							.listRolePolicies(new ListRolePoliciesRequest().withRoleName(role.getRoleName()));
					long timestamp = System.currentTimeMillis();
					for (String policyName : rolePolicies.getPolicyNames()) {
						GetRolePolicyResult rolePolicy = iam.getRolePolicy(
								new GetRolePolicyRequest().withRoleName(role.getRoleName()).withPolicyName(policyName));
						String policyCypher = "match (n:AwsIamRole { aws_arn: {a} })"
								+ "merge (n)-[r:CONTAINS]->(p:AwsInlinePolicy { aws_owner_arn: {a}, aws_policyName: {n} })"
								+ " set p :AwsIamPolicy, p.aws_policyDocument = {d}, r.updateTs = {t}";
						neo4j.execCypher(policyCypher, "a", role.getArn(), "n", policyName, "d",
								urlDecode(rolePolicy.getPolicyDocument()), "t", timestamp);
					}
					String removePolicyCypher = "match (n:AwsIamRole { aws_arn: {a} })-[r:CONTAINS]->(p:AwsInlinePolicy)"
							+ " where r.updateTs < {t} detach delete p";
					neo4j.execCypher(removePolicyCypher, "a", role.getArn(), "t", timestamp);
					
					linkToAccount(role.getArn());
					
				} catch (RuntimeException e) {
					gc.markException(e);
					maybeThrow(e);
				}
			}
			if (result.isTruncated()) {
				result = iam.listRoles(new ListRolesRequest().withMarker(result.getMarker()));
			} else {
				break;
			}
		}
	}

	protected String urlDecode(String s) {
		try {
			return URLDecoder.decode(s, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			return s;
		}
	}

}
