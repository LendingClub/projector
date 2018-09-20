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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.lendingclub.neorx.NeoRxClient;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Strings;

public class NetworkInterfaceScanner extends AbstractEC2Scanner {

	public NetworkInterfaceScanner(AWSScannerBuilder builder) {
		super(builder, "AwsEc2NetworkInterface");
		jsonConverter.withFlattenNestedObjects(true);
	}

	@Override
	protected void doScan() {
		GraphNodeGarbageCollector gc = newGarbageCollector().bindScannerContext();
		getClient().describeNetworkInterfaces().getNetworkInterfaces().forEach(intf -> {
			ObjectNode n = convertAwsObject(intf, getRegion());

			NeoRxClient neo4j = getNeoRxClient();
			try {
				String cypher = "merge (x:AwsEc2NetworkInterface {aws_arn:{arn}}) set x+={props}, x.updateTs=timestamp(), x:CidrBlock return x";
				String arn = n.path("aws_arn").asText();
				n.put("cidrBlock", intf.getPrivateIpAddress() + "/32");
				neo4j.execCypher(cypher, "arn", arn, "props", n).forEach(it -> {
					gc.MERGE_ACTION.accept(it);
				});

				LinkageHelper instanceLinkageHelper = newLinkageHelper().withFromArn(arn)
						.withTargetLabel("AwsEc2Instance").withLinkLabel("ATTACHED_TO");
				if (intf.getAttachment() != null && "attached".equals(intf.getAttachment().getStatus())
						&& !Strings.isNullOrEmpty(intf.getAttachment().getInstanceId())) {
					instanceLinkageHelper.withTargetValues(
							Collections.singletonList(createEc2Arn("instance", intf.getAttachment().getInstanceId())));
				}
				instanceLinkageHelper.execute();

				LinkageHelper subnetLinkageHelper = newLinkageHelper().withFromArn(arn).withTargetLabel("AwsSubnet")
						.withLinkLabel("EXISTS_IN")
						.withTargetValues(Strings.isNullOrEmpty(intf.getSubnetId()) ? Collections.emptyList()
								: Collections.singletonList(createEc2Arn("subnet", intf.getSubnetId())));
				subnetLinkageHelper.execute();

				List<String> securityGroupIds = new ArrayList<>();
				intf.getGroups().forEach(g -> {
					securityGroupIds.add(createEc2Arn("security-group", g.getGroupId()));
				});
				LinkageHelper securityGroupLinkageHelper = newLinkageHelper().withFromArn(arn)
						.withTargetLabel("AwsSecurityGroup").withLinkLabel("SECURED_BY")
						.withTargetValues(securityGroupIds);
				securityGroupLinkageHelper.execute();

				incrementEntityCount();
			} catch (RuntimeException e) {
				gc.markException(e);
				maybeThrow(e);
			}
		});
	}

	@Override
	public Optional<String> computeArn(JsonNode n) {
		return Optional.of(createEc2Arn("network-interface", n.path("aws_networkInterfaceId").asText()));
	}

}
