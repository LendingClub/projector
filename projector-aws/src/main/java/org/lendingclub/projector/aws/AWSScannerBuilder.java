package org.lendingclub.projector.aws;

import java.lang.reflect.InvocationTargetException;

import org.lendingclub.projector.core.Projector;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityResult;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

public class AWSScannerBuilder {

	Projector projector;
	Supplier<String> acccountIdSupplier = Suppliers.memoize(new AccountIdSupplier());
	Region region;
	AWSCredentialsProvider credentialsProvide;
	ClientConfiguration clientConfiguration;

	
	public AWSScannerBuilder() {
		// TODO Auto-generated constructor stub
	}

	AWSCredentialsProvider getCredentialsProvider() {
		if (credentialsProvide==null) {
			return new DefaultAWSCredentialsProviderChain();
		}
		return credentialsProvide;
	}
	class AccountIdSupplier implements Supplier<String> {

		@Override
		public String get() {
			AWSSecurityTokenServiceClientBuilder b = AWSSecurityTokenServiceClientBuilder.standard().withCredentials(getCredentialsProvider());
			if (clientConfiguration!=null) {
				b = b.withClientConfiguration(clientConfiguration);
			}
			AWSSecurityTokenService svc = b.build();
			
		
			GetCallerIdentityResult result = svc.getCallerIdentity(new GetCallerIdentityRequest());
			
			return result.getAccount();
		}
		
	}
	public AWSScannerBuilder withRegion(Regions r) {
		Preconditions.checkState(this.region == null, "region already set");
		this.region = Region.getRegion(r);
		return this;
	}

	public AWSScannerBuilder withRegion(Region r) {
		Preconditions.checkState(this.region == null, "region already set");
		this.region = r;
		return this;
	}

	public AWSScannerBuilder withAccountId(final String id) {
		this.acccountIdSupplier = new Supplier<String>() {

			@Override
			public String get() {
				return id;
			}

			
		};
		return this;
	}

	public AWSScannerBuilder withCredentials(AWSCredentialsProvider p) {
		this.credentialsProvide = p;
		return this;
	}
	
	public AWSScannerBuilder withProjector(Projector p) {
		Preconditions.checkState(this.projector == null, "projector already set");
		this.projector = p;
		return this;
	}

	public AwsClientBuilder configure(AwsClientBuilder b) {
	
		b = b.withRegion(Regions.fromName(region.getName())).withCredentials(getCredentialsProvider());
		if (clientConfiguration != null) {
			b = b.withClientConfiguration(clientConfiguration);
		}
		return b;
	}

	private void checkRequiredState() {
		Preconditions.checkState(projector != null, "projector not set");
		Preconditions.checkState(region != null, "region not set");
	}

	public ASGScanner buildASGScanner() {
		checkRequiredState();
		return new ASGScanner(this);
	}

	public AMIScanner buildAMIScanner() {
		checkRequiredState();
		return new AMIScanner(this);
	}

	public VPCScanner buildVPCScanner() {
		checkRequiredState();
		return new VPCScanner(this);
	}

	public AccountScanner buildAccountScanner() {
		return new AccountScanner(this);

	}


	public SecurityGroupScanner buildSecurityGroupScanner() {
		return build(SecurityGroupScanner.class);
	}

	public SubnetScanner buildSubnetScanner() {
		return build(SubnetScanner.class);
	}

	public RDSInstanceScanner buildRDSInstanceScanner() {
		return build(RDSInstanceScanner.class);
	}

	public <T> T build(Class<T> clazz) {
		try {
			return (T) clazz.getConstructor(AWSScannerBuilder.class).newInstance(this);
		} catch (IllegalAccessException | InstantiationException | InvocationTargetException
				| NoSuchMethodException e) {
			throw new IllegalStateException(e);
		}

	}
}
