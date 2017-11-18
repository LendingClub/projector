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
