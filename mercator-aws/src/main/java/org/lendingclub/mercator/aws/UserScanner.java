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
