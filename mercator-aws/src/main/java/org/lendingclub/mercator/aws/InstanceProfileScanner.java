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

import java.util.stream.Collectors;

import org.lendingclub.neorx.NeoRxClient;

import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient;
import com.amazonaws.services.identitymanagement.model.InstanceProfile;
import com.amazonaws.services.identitymanagement.model.ListInstanceProfilesRequest;
import com.amazonaws.services.identitymanagement.model.ListInstanceProfilesResult;
import com.amazonaws.services.identitymanagement.model.Role;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class InstanceProfileScanner extends IAMScanner {

	public InstanceProfileScanner(AWSScannerBuilder builder) {
		super(builder, "AwsInstanceProfile");
	}

	@Override
	protected void doGlobalScan() {
		AmazonIdentityManagementClient iam = getClient();
		ListInstanceProfilesRequest request = new ListInstanceProfilesRequest();
		GraphNodeGarbageCollector gc = newGarbageCollector().bindScannerContext();
		while (true) {
			ListInstanceProfilesResult instanceProfiles = iam.listInstanceProfiles(request);
			instanceProfiles.getInstanceProfiles().forEach(n -> scanInstanceProfile(gc, n));

			if (!instanceProfiles.isTruncated()) {
				break;
			}
			request.setMarker(instanceProfiles.getMarker());
		}
	}

	private void scanInstanceProfile(GraphNodeGarbageCollector gc, InstanceProfile instanceProfile) {
		ObjectNode n = convertAwsObject(instanceProfile, null);
		NeoRxClient neo4j = getNeoRxClient();
		String cypher = "merge (n:AwsInstanceProfile { aws_arn: {a} }) set n += {p}, n.updateTs = timestamp() return n";
		neo4j.execCypher(cypher, "a", instanceProfile.getArn(), "p", n).forEach(it -> {
			gc.MERGE_ACTION.accept(it);
		});
		incrementEntityCount();
		LinkageHelper linkage = newLinkageHelper();
		linkage.withTargetLabel("AwsIamRole").withFromArn(instanceProfile.getArn()).withLinkLabel("HAS_ROLE")
				.withTargetValues(instanceProfile.getRoles().stream().map(Role::getArn).collect(Collectors.toList()))
				.execute();
	}

}
