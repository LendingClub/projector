package org.lendingclub.mercator.aws;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.lendingclub.neorx.NeoRxClient;

import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient;
import com.amazonaws.services.identitymanagement.model.ListEntitiesForPolicyRequest;
import com.amazonaws.services.identitymanagement.model.ListEntitiesForPolicyResult;
import com.amazonaws.services.identitymanagement.model.ListPoliciesRequest;
import com.amazonaws.services.identitymanagement.model.ListPoliciesResult;
import com.amazonaws.services.identitymanagement.model.Policy;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ManagedPolicyScanner extends IAMScanner {

	public ManagedPolicyScanner(AWSScannerBuilder builder) {
		super(builder, "AwsIamManagedPolicy");
	}

	@Override
	protected void doGlobalScan() {
		AmazonIdentityManagementClient iam = getClient();
		NeoRxClient neo4j = getNeoRxClient();
		GraphNodeGarbageCollector gc = newGarbageCollector().bindScannerContext();
		ListPoliciesRequest request = new ListPoliciesRequest();
		do {
			ListPoliciesResult policies = iam.listPolicies(request);
			for (Policy policy : policies.getPolicies()) {

				boolean awsManagedPolicy;

				if ((awsManagedPolicy = policy.getArn().startsWith("arn:aws:iam::aws:policy"))
						&& policy.getAttachmentCount() != null && policy.getAttachmentCount() == 0) {
					// don't record AWS managed policies unless they're attached to something
					continue;
				}

				ObjectNode n = convertAwsObject(policy, getRegion());
				if (awsManagedPolicy) {
					n.remove("aws_account");
				}

				String cypher = "merge (n:AwsIamManagedPolicy { aws_arn: {a} }) set n += {p}, n.updateTs = timestamp(),"
						+ " n :AwsIamPolicy return n";
				neo4j.execCypher(cypher, "a", policy.getArn(), "p", n).forEach(gc.MERGE_ACTION::accept);
				incrementEntityCount();
				linkToAccount(policy.getArn());

				updateAttachedEntities(iam, policy);

			}
			if (policies.isTruncated()) {
				request.setMarker(policies.getMarker());
			} else {
				break;
			}
		} while (true);

	}

	private void updateAttachedEntities(AmazonIdentityManagementClient iam, Policy policy) {
		ListEntitiesForPolicyRequest request = new ListEntitiesForPolicyRequest()
				.withPolicyArn(policy.getArn());
		List<String> attachedRoles = new ArrayList<>();
		List<String> attachedUsers = new ArrayList<>();
		while (true) {
			ListEntitiesForPolicyResult attachedEntities = iam.listEntitiesForPolicy(request);
			attachedRoles.addAll(attachedEntities.getPolicyRoles().stream()
					.map(rp -> createIamArn("role", rp.getRoleName())).collect(Collectors.toList()));
			attachedUsers.addAll(attachedEntities.getPolicyUsers().stream()
					.map(ur -> createIamArn("user", ur.getUserName())).collect(Collectors.toList()));

			// TODO - group
			if (!attachedEntities.isTruncated()) {
				break;
			}
			request.setMarker(attachedEntities.getMarker());
		}
		if (!attachedRoles.isEmpty()) {
			newLinkageHelper().withLinkLabel("ATTACHED_TO").withFromArn(policy.getArn())
					.withTargetLabel("AwsIamRole").withTargetValues(attachedRoles).execute();
		}
		if (!attachedUsers.isEmpty()) {
			newLinkageHelper().withLinkLabel("ATTACHED_TO").withFromArn(policy.getArn())
					.withTargetLabel("AwsIamUser").withTargetValues(attachedUsers).execute();
		}
	}

}
