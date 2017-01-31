package org.lendingclub.projector.aws;

import com.amazonaws.AmazonWebServiceClient;

public class AllEntityScanner extends AWSScanner<AmazonWebServiceClient> {

	public AllEntityScanner(AWSScannerBuilder builder) {
		super(builder);

	}

	@Override
	protected AmazonWebServiceClient createClient() {
	
		return null;
	}

	@Override
	public void doScan() {
	
		builder.build(AccountScanner.class).scan();
		builder.build(RegionScanner.class).scan();
		builder.build(AvailabilityZoneScanner.class).scan();
		builder.build(SubnetScanner.class).scan();
		builder.build(VPCScanner.class).scan();
		builder.build(SecurityGroupScanner.class).scan();
		builder.build(AMIScanner.class).scan();
		builder.build(EC2InstanceScanner.class).scan();
		builder.build(ELBScanner.class).scan();
		builder.build(LaunchConfigScanner.class).scan();
		builder.build(ASGScanner.class).scan();
		builder.build(RDSInstanceScanner.class).scan();
		
	}



}
