package org.lendingclub.mercator.aws;

import com.amazonaws.AmazonWebServiceClient;

public abstract class GlobalAWSScanner<T extends AmazonWebServiceClient> extends AWSScanner<T> {

	public GlobalAWSScanner(AWSScannerBuilder builder, Class<T> clientType, String label) {
		super(builder, clientType, label);
	}

	@Override
	protected final void doScan() {
		if (builder.isIncludeGlobalResources()) {
			doGlobalScan();
		}
	}

	protected abstract void doGlobalScan();

}
