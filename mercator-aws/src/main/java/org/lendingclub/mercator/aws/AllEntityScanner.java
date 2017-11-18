package org.lendingclub.mercator.aws;

public class AllEntityScanner extends AWSParallelScannerGroup {

	public AllEntityScanner(AWSScannerBuilder builder) {
		super(builder);

		addScannerType(Route53Scanner.class);
		addScannerType(IAMScannerGroup.class);
		
		addScannerType(EC2ScannerGroup.class);
		addScannerType(ASGScannerGroup.class);
		addScannerType(ELBScanner.class);
		addScannerType(RDSInstanceScanner.class);
		addScannerType(S3BucketScanner.class);
		addScannerType(SQSScanner.class);
		addScannerType(SNSScanner.class);

		// addScannerType(KinesisScanner.class); // Kinesis seems to be heavily
		// rate-limited...leave it off for now
	}

}
