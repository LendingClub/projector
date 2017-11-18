package org.lendingclub.mercator.aws;

import java.util.Optional;

import org.lendingclub.neorx.NeoRxClient;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.VpcPeeringConnection;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class VPCPeeringConnectionScanner extends AbstractEC2NetworkInfrastructureScanner {

	public VPCPeeringConnectionScanner(AWSScannerBuilder builder) {
		super(builder, "AwsVpcPeeringConnection");
		jsonConverter.flattenNestedObjects = true;
	}

	@Override
	public int[] getSlowScanRatio() {
		return new int[] { 1, 30 };
	}

	@Override
	protected void doScan() {
		AmazonEC2Client ec2 = getClient();
		GraphNodeGarbageCollector gc = newGarbageCollector().bindScannerContext();
		ec2.describeVpcPeeringConnections().getVpcPeeringConnections().forEach(c -> scanConnection(gc, c));
	}

	@Override
	public Optional<String> computeArn(JsonNode n) {
		return Optional.of(createEc2Arn("vpc-peering-connection", n.path("aws_vpcPeeringConnectionId").asText()));
	}

	private void scanConnection(GraphNodeGarbageCollector gc, VpcPeeringConnection peeringConnection) {
		NeoRxClient neo4j = getNeoRxClient();
		ObjectNode n = convertAwsObject(peeringConnection, getRegion());
		neo4j.execCypher("merge (n:AwsVpcPeeringConnection { aws_arn: {a} }) set n += {p} return n", "a",
				n.path("aws_arn"), "p", n).forEach(gc.MERGE_ACTION);
		incrementEntityCount();
	}

}
