package org.lendingclub.mercator.aws;

public class ASGScannerGroup extends AWSScannerGroup {

	public ASGScannerGroup(AWSScannerBuilder builder) {
		super(builder);
		addScannerType(LaunchConfigScanner.class);
		addScannerType(ASGScanner.class);
	
	}

}
