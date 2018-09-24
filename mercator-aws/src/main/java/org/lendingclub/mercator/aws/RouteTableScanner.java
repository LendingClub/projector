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

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.lendingclub.neorx.NeoRxClient;

import com.amazonaws.services.ec2.model.Route;
import com.amazonaws.services.ec2.model.RouteTable;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Strings;

public class RouteTableScanner extends AbstractEC2NetworkInfrastructureScanner {

	public RouteTableScanner(AWSScannerBuilder builder) {
		super(builder, "AwsRouteTable");
	}

	@Override
	protected void doScan() {
		GraphNodeGarbageCollector gc = newGarbageCollector().bindScannerContext();
		getClient().describeRouteTables().getRouteTables().forEach(rt -> {

			ObjectNode n = convertAwsObject(rt, getRegion());

			NeoRxClient neo4j = getNeoRxClient();
			try {
				String cypher = "merge (x:AwsRouteTable {aws_arn:{arn}}) set x+={props}, x.updateTs=timestamp() return x";
				String arn = n.path("aws_arn").asText();
				neo4j.execCypher(cypher, "arn", arn, "props", n).forEach(it -> {
					gc.MERGE_ACTION.accept(it);
				});
				incrementEntityCount();

				LinkageHelper subnetLinkage = newLinkageHelper().withFromArn(arn).withTargetLabel("AwsSubnet")
						.withLinkLabel("ATTACHED_TO")
						.withTargetValues(rt.getAssociations().stream()
								.filter(a -> !Strings.isNullOrEmpty(a.getSubnetId()))
								.map(a -> createEc2Arn("subnet", a.getSubnetId())).collect(Collectors.toList()));
				subnetLinkage.execute();

				updateRoutes(rt, n.path("aws_arn").asText());

			} catch (RuntimeException e) {
				gc.markException(e);
				maybeThrow(e);
			}
		});
	}

	private void updateRoutes(RouteTable rt, String arn) {
		/*
		 * TODO - consider switching this to a similar structure that the
		 * SecurityGroupScanner produces; that is generate AwsRoute nodes, then
		 * associate the route nodes to destination cidr blocks.
		 */
		Set<String> vpcEndpoints = new HashSet<>();
		Set<String> vpnGateways = new HashSet<>();
		Set<String> internetGateways = new HashSet<>();
		Set<String> peeringConnections = new HashSet<>();
		Set<String> cidrBlocks = new HashSet<>();
		for (Route r : rt.getRoutes()) {
			if (r.getState().equals("blackhole")) {
				continue;
			}
			if (!Strings.isNullOrEmpty(r.getGatewayId())) {
				String id = r.getGatewayId();
				if (id.startsWith("vpce-")) {
					vpcEndpoints.add(id);
				} else if (id.startsWith("vgw-")) {
					vpnGateways.add(id);
				} else if (id.startsWith("igw-")) {
					internetGateways.add(id);
				}
			}
			if (!Strings.isNullOrEmpty(r.getVpcPeeringConnectionId())) {
				peeringConnections.add(r.getVpcPeeringConnectionId());
			}
			if (!Strings.isNullOrEmpty(r.getDestinationCidrBlock())) {
				cidrBlocks.add(r.getDestinationCidrBlock());
			}
		}
		routeTo(arn, vpcEndpoints, "AwsVpcEndpoint", "aws_vpcEndpointId", true);
		routeTo(arn, vpnGateways, "AwsVpnGateway", "aws_vpnGatewayId", true);
		routeTo(arn, internetGateways, "AwsInternetGateway", "aws_internetGatewayId", true);
		routeTo(arn, peeringConnections, "AwsVpcPeeringConnection", "aws_vpcPeeringConnectionId", true);
		routeTo(arn, cidrBlocks, "CidrBlock", "cidrBlock", false);
	}

	private void routeTo(String arn, Collection<String> ids, String targetLabel, String targetLinkAttribute,
			boolean localRegion) {
		if (!ids.isEmpty()) {
			LinkageHelper linkage = newLinkageHelper().withFromArn(arn).withLinkLabel("ROUTES_TO")
					.withTargetLabel(targetLabel).withTargetLinkAttribute(targetLinkAttribute).withTargetValues(ids);
			if (localRegion) {
				linkage.withMoreWhere("b.aws_region = {R}", "R", getRegion().getName());
			}
			linkage.execute();
		}
	}

	@Override
	public Optional<String> computeArn(JsonNode n) {
		return Optional.of(createEc2Arn("route-table", n.path("aws_routeTableId").asText()));
	}

}
