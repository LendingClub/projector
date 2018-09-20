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

import static com.google.common.base.Charsets.UTF_8;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.lendingclub.neorx.NeoRxClient;

import com.amazonaws.services.ec2.model.DescribeSecurityGroupsRequest;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsResult;
import com.amazonaws.services.ec2.model.IpPermission;
import com.amazonaws.services.ec2.model.IpRange;
import com.amazonaws.services.ec2.model.Ipv6Range;
import com.amazonaws.services.ec2.model.SecurityGroup;
import com.amazonaws.services.ec2.model.UserIdGroupPair;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

public class SecurityGroupScanner extends AbstractEC2Scanner {

	private final static int VERSION = 7;

	public SecurityGroupScanner(AWSScannerBuilder builder) {
		super(builder, "AwsSecurityGroup");
	}

	@Override
	public Optional<String> computeArn(JsonNode n) {
		// arn:aws:ec2:region:account-id:security-group/security-group-id
		return Optional.of(String.format("arn:aws:ec2:%s:%s:security-group/%s", n.get("aws_region").asText(),
				n.get("aws_account").asText(), n.get("aws_groupId").asText()));
	}

	@Override
	protected void doScan() {
		rateLimit();
		DescribeSecurityGroupsResult result = getClient().describeSecurityGroups();
		long now = System.currentTimeMillis();
		GraphNodeGarbageCollector gc = newGarbageCollector().bindScannerContext();
		while (true) {
			processSecurityGroups(gc, result, now);
			if (result.getNextToken() == null) {
				break;
			}
			result = getClient()
					.describeSecurityGroups(new DescribeSecurityGroupsRequest().withNextToken(result.getNextToken()));
		}

	}

	private void processSecurityGroups(GraphNodeGarbageCollector gc, DescribeSecurityGroupsResult result, long now) {
		result.getSecurityGroups().forEach(sg -> {
			try {
				ObjectNode g = convertAwsObject(sg, getRegion());

				String arn = g.path(AWS_ARN_ATTRIBUTE).asText();
				String accessRulesHash = getRulesHash(sg);
				g.put("accessRulesHash", accessRulesHash);
				String previousAccessRulesHash = getNeoRxClient()
						.execCypher("match (sg:AwsSecurityGroup { aws_arn: {arn} }) return sg.accessRulesHash", "arn",
								arn)
						.blockingFirst(MissingNode.getInstance()).asText();

				// non-VPC security groups don't have a VPC
				String vpcId = Strings.nullToEmpty(sg.getVpcId());
				String cypher = "merge (sg:AwsSecurityGroup {aws_arn:{arn}}) set sg+={props}, sg.updateTs={now} return sg";

				JsonNode sgNode = getNeoRxClient().execCypher(cypher, "arn", arn, "props", g, "now", now)
						.blockingFirst();
				getShadowAttributeRemover().removeTagAttributes("AwsSecurityGroup", g, sgNode);
				gc.updateEarliestTimestamp(sgNode);
				if (!vpcId.isEmpty()) {
					cypher = "match (v:AwsVpc {aws_vpcId: {vpcId}}), (sg:AwsSecurityGroup {aws_arn:{sg_arn}}) merge (sg)-[:RESIDES_IN]->(v)";
					getNeoRxClient().execCypher(cypher, "vpcId", vpcId, "sg_arn", g.path("aws_arn").asText());
				}
				incrementEntityCount();

				if (accessRulesHash.equals(previousAccessRulesHash)) {
					logger.debug("security group rules are unchanged");
				} else {
					updateAccessRules(sg, arn);
				}
			} catch (RuntimeException e) {
				maybeThrow(e, "problem scanning security groups");
			}
		});
	}

	private void updateAccessRules(SecurityGroup sg, String arn) {
		NeoRxClient neo4j = getNeoRxClient();
		/*
		 * If anything changed we're just going to clobber all the relationships and
		 * start from scratch.
		 */
		neo4j.execCypher("match (sg:AwsSecurityGroup { aws_arn: {arn} })--(r:AwsAccessRule) detach delete r", "arn",
				arn);
		sg.getIpPermissions().forEach(p -> createAccessRules(sg, arn, "AwsInboundAccessRule", "INBOUND_RULE", p));
		sg.getIpPermissionsEgress()
				.forEach(p -> createAccessRules(sg, arn, "AwsOutboundAccessRule", "OUTBOUND_RULE", p));
	}

	private void createAccessRules(SecurityGroup sg, String arn, String label, String linkLabel, IpPermission p) {
		/*
		 * We are going to mimic the presentation in the AWS console rather than what we
		 * get back from the API -- we are going to denormalize the IpPermssion
		 * structure into access rules.
		 */
		ObjectNode baseNode = mapper.createObjectNode();
		baseNode.put("aws_fromPort", nullToZero(p.getFromPort()));
		baseNode.put("aws_toPort", nullToZero(p.getToPort()));
		baseNode.put("aws_ipProtocol", Strings.nullToEmpty(p.getIpProtocol()));
		for (IpRange ip : p.getIpv4Ranges()) {
			ObjectNode ruleNode = baseNode.deepCopy();
			ruleNode.put("aws_cidrIp", Strings.nullToEmpty(ip.getCidrIp()));
			addDescription(ruleNode, ip.getDescription(), "aws_cidrIp");
			long ruleId = createAccessRule(arn, label, linkLabel, ruleNode);
			if (!Strings.isNullOrEmpty(ip.getCidrIp())) {
				// relate to anything tagged with CidrBlock
				String cidrBlockCypher = "match (r:AwsAccessRule), (s:CidrBlock { cidrBlock: {cidrBlock} }) "
						+ " where id(r) = {ruleId} create (r)-[:ALLOWS_ACCESS]->(s)";
				getNeoRxClient().execCypher(cidrBlockCypher, "cidrBlock", ip.getCidrIp(), "ruleId", ruleId);
			}
			
		}
		for (Ipv6Range ip : p.getIpv6Ranges()) {
			ObjectNode ruleNode = baseNode.deepCopy();
			ruleNode.put("aws_cidrIpv6", Strings.nullToEmpty(ip.getCidrIpv6()));
			addDescription(ruleNode, ip.getDescription(), "aws_cidrIpv6");
			createAccessRule(arn, label, linkLabel, ruleNode);
		}
		for (UserIdGroupPair uigp : p.getUserIdGroupPairs()) {
			ObjectNode ruleNode = baseNode.deepCopy();
			addDescription(ruleNode, uigp.getDescription(), null);
			putOptionalString(ruleNode, "aws_groupId", uigp.getGroupId());
			// not going to record peering status since we already have that elsewhere
			putOptionalString(ruleNode, "aws_userId", uigp.getUserId());
			putOptionalString(ruleNode, "aws_vpcId", uigp.getVpcId());
			putOptionalString(ruleNode, "aws_vpcPeeringConnectionId", uigp.getVpcPeeringConnectionId());
			long ruleId = createAccessRule(arn, label, linkLabel, ruleNode);
			if (!Strings.isNullOrEmpty(uigp.getGroupId())) {
				// relate to any security groups
				String cypher = "match (r:AwsAccessRule), "
						+ "(sg:AwsSecurityGroup { aws_region: {region}, aws_groupId: {groupId} })"
						+ " where id(r) = {ruleId} " + " create (r)-[:ALLOWS_ACCESS]->(sg)";
				getNeoRxClient().execCypher(cypher, "region", getRegion().getName(), "groupId", uigp.getGroupId(),
						"ruleId", ruleId);
			}
		}

	}

	private void addDescription(ObjectNode ruleNode, String description, String extraDescriptionField) {
		if (Strings.isNullOrEmpty(description)) {
			/*
			 * If the description is empty we will auto-generate one.
			 */
			StringBuilder s = new StringBuilder();
			s.append(ruleNode.path("aws_ipProtocol").asText()).append(" ")
					.append(ruleNode.path("aws_fromPort").asText()).append("-").append(ruleNode.path("aws_toPort"));
			if (extraDescriptionField != null) {
				s.append(" ").append(ruleNode.path(extraDescriptionField).asText());
			}
			description = s.toString();
		}
		ruleNode.put("aws_description", description);
	}

	private void putOptionalString(ObjectNode ruleNode, String field, String value) {
		if (!Strings.isNullOrEmpty(value)) {
			ruleNode.put(field, value);
		}
	}

	private long createAccessRule(String arn, String label, String linkLabel, ObjectNode ruleNode) {
		String cypher = "match (sg:AwsSecurityGroup { aws_arn: {arn} }) create (r:AwsAccessRule)"
				+ " set r += {props}, r:" + label + " create (sg)-[:" + linkLabel + "]->(r) return id(r)";
		return getNeoRxClient().execCypher(cypher, "arn", arn, "props", ruleNode).blockingFirst().longValue();
	}

	private static int nullToZero(Integer i) {
		return i == null ? 0 : i.intValue();
	}

	@VisibleForTesting
	static String getRulesHash(SecurityGroup sg) {
		/*
		 * We're going to hash to toString() of the security group. This is somewhat
		 * imprecise, but much simpler than iterating through the nested structures
		 * here.
		 * 
		 * We start with a VERSION constant that can be bumped up in code to force a
		 * refresh of the rules in case the code changes.
		 */
		Hasher h = Hashing.sha1().newHasher().putInt(VERSION);
		/*
		 * It turns out that AWS does not return permissions in a consistent order, so
		 * force an order on them.
		 */
		sortPermissions(sg.getIpPermissions());
		sortPermissions(sg.getIpPermissionsEgress());
		h.putString(sg.toString(), UTF_8);
		return h.hash().toString();
	}

	private static void sortPermissions(List<IpPermission> permissions) {
		permissions.sort(Comparator.comparing(coalesce(IpPermission::getFromPort, 0))
				.thenComparing(Comparator.comparing(coalesce(IpPermission::getToPort, 0)))
				.thenComparing(Comparator.comparing(coalesce(IpPermission::getIpProtocol, ""))));
		permissions.forEach(SecurityGroupScanner::sortPermissionContents);
	}

	private static <A, T> Function<A, T> coalesce(Function<A, T> k, T defaultValue) {
		return value -> {
			T result = k.apply(value);
			return result == null ? defaultValue : result;
		};
	}

	private static void sortPermissionContents(IpPermission p) {
		p.getIpv4Ranges().sort(Comparator.comparing(coalesce(IpRange::getCidrIp, ""))
				.thenComparing(coalesce(IpRange::getDescription, "")));
		p.getIpv6Ranges().sort(Comparator.comparing(coalesce(Ipv6Range::getCidrIpv6, ""))
				.thenComparing(coalesce(Ipv6Range::getDescription, "")));
		p.getUserIdGroupPairs().sort(Comparator.comparing(coalesce(UserIdGroupPair::getDescription, ""))
				.thenComparing(coalesce(UserIdGroupPair::getGroupId, ""))
				.thenComparing(coalesce(UserIdGroupPair::getUserId, "")).thenComparing(UserIdGroupPair::getVpcId)
				.thenComparing(coalesce(UserIdGroupPair::getVpcPeeringConnectionId, "")));

	}

}
