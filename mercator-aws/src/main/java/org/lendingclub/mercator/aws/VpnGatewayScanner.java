package org.lendingclub.mercator.aws;

import java.util.Optional;

import org.lendingclub.neorx.NeoRxClient;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.VpnGateway;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class VpnGatewayScanner extends AbstractEC2NetworkInfrastructureScanner {

	public VpnGatewayScanner(AWSScannerBuilder builder) {
		super(builder, "AwsVpnGateway");
	}
	
	@Override
	protected void doScan() {
		AmazonEC2Client ec2 = getClient();
		GraphNodeGarbageCollector gc = newGarbageCollector().bindScannerContext();
		ec2.describeVpnGateways().getVpnGateways().forEach(c -> scanVpnGateway(gc, c));
	}

	@Override
	public Optional<String> computeArn(JsonNode n) {
		return Optional.of(createEc2Arn("vpn-gateway", n.path("aws_vpnGatewayId").asText()));
	}

	private void scanVpnGateway(GraphNodeGarbageCollector gc, VpnGateway c) {
		NeoRxClient neo4j = getNeoRxClient();
		ObjectNode n = convertAwsObject(c, getRegion());
		neo4j.execCypher("merge (n:AwsVpnGateway { aws_arn: {a} }) set n += {p} return n", "a",
				n.path("aws_arn"), "p", n).forEach(gc.MERGE_ACTION);
		incrementEntityCount();
	}

}
