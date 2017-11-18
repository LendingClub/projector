package org.lendingclub.mercator.aws;

public abstract class AbstractEC2NetworkInfrastructureScanner extends AbstractEC2Scanner implements AWSSlowScan {

	public AbstractEC2NetworkInfrastructureScanner(AWSScannerBuilder builder, String label) {
		super(builder, label);
		jsonConverter.withFlattenNestedObjects(true);
	}

	@Override
	public int[] getSlowScanRatio() {
		return new int[] { 1, 30 };
	}


}
