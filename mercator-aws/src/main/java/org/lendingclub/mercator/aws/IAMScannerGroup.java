package org.lendingclub.mercator.aws;

public class IAMScannerGroup extends AWSScannerGroup {

	public IAMScannerGroup(AWSScannerBuilder builder) {
		super(builder);
		
		addScannerType(UserScanner.class);
		addScannerType(RoleScanner.class);
		addScannerType(InstanceProfileScanner.class);
		addScannerType(ManagedPolicyScanner.class);

	}

}
