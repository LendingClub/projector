package org.lendingclub.projector.aws;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;

public abstract class AbstractEC2Scanner extends AWSScanner<AmazonEC2Client> {

	public AbstractEC2Scanner(AWSScannerBuilder builder) {
		super(builder);

	}


	
	protected AmazonEC2Client createClient() {
		
		
		AmazonEC2Client client =  (AmazonEC2Client) builder.configure(AmazonEC2ClientBuilder
				.standard()).build();
		
	
	
		return client;
		
	}



	

}
