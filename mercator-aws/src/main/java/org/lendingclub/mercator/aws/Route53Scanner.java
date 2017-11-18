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

import org.lendingclub.mercator.core.ScannerContext;

import com.amazonaws.services.route53.AmazonRoute53Client;
import com.amazonaws.services.route53.model.GetHostedZoneRequest;
import com.amazonaws.services.route53.model.GetHostedZoneResult;
import com.amazonaws.services.route53.model.HostedZone;
import com.amazonaws.services.route53.model.ListHostedZonesRequest;
import com.amazonaws.services.route53.model.ListHostedZonesResult;
import com.amazonaws.services.route53.model.ListResourceRecordSetsRequest;
import com.amazonaws.services.route53.model.ListResourceRecordSetsResult;
import com.amazonaws.services.route53.model.ResourceRecord;
import com.amazonaws.services.route53.model.ResourceRecordSet;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class Route53Scanner extends GlobalAWSScanner<AmazonRoute53Client> implements AWSSlowScan {

	public Route53Scanner(AWSScannerBuilder builder) {
		super(builder, AmazonRoute53Client.class,"AwsRoute53HostedZone");

	}

	/**
	 * This scanner is particularly slow for largish hosted zones.
	 */
	@Override
	public int[] getSlowScanRatio() {
		return new int[] { 1, 17 };
	}

	@Override
	protected void doGlobalScan() {

		ListHostedZonesRequest request = new ListHostedZonesRequest();
		ListHostedZonesResult result = new ListHostedZonesResult();

		do {
			rateLimit();
			result = getClient().listHostedZones(request);
			for (HostedZone zone : result.getHostedZones()) {
				scanHostedZoneById(zone.getId());
			}
			request.setMarker(result.getMarker());
		} while (result.isTruncated());

	}

	public void scanHostedZoneById(String id) {

		GetHostedZoneRequest request = new GetHostedZoneRequest();
		request.setId(id);

		rateLimit();
		GetHostedZoneResult result = getClient().getHostedZone(request);

		projectHostedZoneResult(result);
	}

	ObjectNode toJson(GetHostedZoneResult hzResult) {
		HostedZone hz = hzResult.getHostedZone();

		ObjectNode n = mapper.createObjectNode();
		n.put("aws_account", getAccountId());
		n.put("aws_id", hz.getId());
		n.put("aws_name", hz.getName());
		n.put("aws_callerReference", hz.getCallerReference());
		n.put("aws_resourceRecordSetCount", hz.getResourceRecordSetCount());
		n.put("aws_comment", hz.getConfig().getComment());
		n.put("aws_privateZone", hz.getConfig().getPrivateZone());
		n.put("aws_arn", "arn:aws:route53:::hostedzone/" + hz.getId());

		ArrayNode an = mapper.createArrayNode();
		if (hzResult.getDelegationSet() != null) {
			hzResult.getDelegationSet().getNameServers().forEach(ns -> {
				an.add(ns);
			});
		}
		n.set("aws_nameServers", an);
		return n;

	}

	ObjectNode toJson(ResourceRecordSet rs) {
		ObjectNode n = mapper.createObjectNode();
		n.put("aws_ttl", rs.getTTL());
		n.put("aws_type", rs.getType());
		n.put("aws_name", rs.getName());
		n.put("id", rs.getType() + "_" + rs.getName());
		ArrayNode an = mapper.createArrayNode();
		n.set("aws_resourceRecords", an);

		for (ResourceRecord rr : rs.getResourceRecords()) {
			an.add(rr.getValue());
		}
		return n;
	}

	protected void projectResourceRecordSet(String hostedZoneId, ResourceRecordSet rs, long timestamp) {

		ObjectNode n = toJson(rs);

		getNeoRxClient().execCypher(
				"merge (a:AwsRoute53RecordSet {aws_name:{aws_name}}) set a+={props}, a.updateTs={updateTs} return a",
				"aws_name", n.get("aws_name").asText(), "props", n, "updateTs", timestamp);

		getNeoRxClient().execCypher(
				"match (r:AwsRoute53RecordSet {aws_name:{aws_name}}), (z:AwsRoute53HostedZone {aws_id : {aws_id}}) merge (z)-[x:CONTAINS]->(r) set x.updateTs={updateTs}",
				"aws_name", rs.getName(), "aws_id", hostedZoneId, "updateTs", timestamp);
		
		ScannerContext.getScannerContext().ifPresent(sc -> {
			sc.incrementEntityCount();
		});

	}

	protected void projectHostedZoneResult(GetHostedZoneResult hostedZoneResult) {

		HostedZone hz = hostedZoneResult.getHostedZone();
		ObjectNode n = toJson(hostedZoneResult);

		getNeoRxClient().execCypher(
				"merge (a:AwsRoute53HostedZone {aws_id:{aws_id}}) set a+={props}, a.updateTs=timestamp() return a",
				"aws_id", n.get("aws_id").asText(), "props", n);

		ListResourceRecordSetsRequest request = new ListResourceRecordSetsRequest();
		request.setHostedZoneId(hz.getId());
		ListResourceRecordSetsResult result;

		long timestamp = System.currentTimeMillis();

		do {
			rateLimit();
			result = getClient().listResourceRecordSets(request);
			request.setStartRecordName(result.getNextRecordName());

			for (ResourceRecordSet rs : result.getResourceRecordSets()) {

				projectResourceRecordSet(hz.getId(), rs, timestamp);

			}

		} while (result.isTruncated());

		getNeoRxClient().execCypher(
				"match (z:AwsRoute53HostedZone {aws_id:{aws_id}})--(r:AwsRoute53RecordSet) where r.updateTs<{ts} detach delete r",
				"ts", timestamp, "aws_id", hz.getId());
		getNeoRxClient().execCypher(
				"match (a:AwsRoute53RecordSet) where not (a)-[:CONTAINS]-(:AwsRoute53HostedZone) detach delete a");
	}

}
