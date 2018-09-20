/**
 * Copyright 2017-2018 LendingClub, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lendingclub.mercator.aws;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.atomic.AtomicReference;

import org.lendingclub.mercator.core.Projector;
import org.lendingclub.mercator.core.ScannerBuilder;

import com.amazonaws.AmazonWebServiceClient;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
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

@SuppressWarnings("rawtypes")
public class AWSScannerBuilder extends ScannerBuilder<AWSScanner> {

	private Supplier<String> acccountIdSupplier = Suppliers.memoize(new AccountIdSupplier());
	private Region region;
	private AWSCredentialsProvider credentialsProvider;
	private ClientConfiguration clientConfiguration;
	private Class<? extends AWSScanner> targetType;
	private boolean includeGlobalResources;
	private AtomicReference<AWSParallelScannerGroup> initialScannerGroup = new AtomicReference<>();

	public AWSScannerBuilder() {
	}

	AWSScannerBuilder(AWSScannerBuilder builder) {
		clientConfiguration = builder.clientConfiguration;
		initialScannerGroup = builder.initialScannerGroup;
		setProjector(builder.getProjector());
		withFailOnError(builder.isFailOnError());
		if (builder.getRateLimitPerSecond().isPresent()) {
			withRateLimitPerSecond(builder.getRateLimitPerSecond().get());
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public AWSScannerBuilder withRateLimitPerSecond(double c) {
		return (AWSScannerBuilder) super.withRateLimitPerSecond(c);
	}

	AWSCredentialsProvider getCredentialsProvider() {
		if (credentialsProvider == null) {
			return new DefaultAWSCredentialsProviderChain();
		}
		return credentialsProvider;
	}

	public Supplier<String> getAccountIdSupplier() {
		return acccountIdSupplier;
	}

	class AccountIdSupplier implements Supplier<String> {

		@Override
		public String get() {
			AWSSecurityTokenServiceClientBuilder b = AWSSecurityTokenServiceClientBuilder.standard()
					.withCredentials(getCredentialsProvider());

			// this will fail if the region is not set
			if (region != null) {
				b = b.withRegion(region.getName());
			} else {
				b = b.withRegion(Regions.US_EAST_1);
			}
			if (clientConfiguration != null) {
				b = b.withClientConfiguration(clientConfiguration);
			}
			AWSSecurityTokenService svc = b.build();

			GetCallerIdentityResult result = svc.getCallerIdentity(new GetCallerIdentityRequest());

			return result.getAccount();
		}

	}

	public AWSScannerBuilder withRegion(String region) {
		return withRegion(Regions.fromName(region));
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

	public AWSScannerBuilder withIncludeGlobalResources(boolean value) {
		this.includeGlobalResources = value;
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

	/**
	 * This is more of a testing convenience than anything else. Credentials are
	 * loaded using the DefaultAWSCredentialsProviderChain, and then an alternate
	 * role is assumed.
	 * 
	 * @param roleArn
	 *            the role that we are assuming.
	 * @param sessionName
	 *            the session name is user-chosen...usefull for auditing/logging
	 * @return
	 */
	public AWSScannerBuilder withAssumeRoleCredentials(String roleArn, String sessionName) {

		// If we wanted to be able to use a different credentials provider, we could. We
		// would just need to initialize a
		// AWSSecurityTokenService and pass it to the
		// STSAssumeRoleSessionCredentialsProvier.Builder. I am leaving the code here
		// just for reference, because
		// it is really easy to forget.

		// AWSSecurityTokenService sts =
		// AWSSecurityTokenServiceClientBuilder.standard().withCredentials(new
		// DefaultAWSCredentialsProviderChain()).build();

		AWSCredentialsProvider p = new STSAssumeRoleSessionCredentialsProvider.Builder(roleArn, sessionName).build();

		return withCredentials(p);

	}


	public AWSScannerBuilder withCredentials(AWSCredentialsProvider p) {
		this.credentialsProvider = p;
		return this;
	}

	public AWSScannerBuilder withProjector(Projector p) {
		Preconditions.checkState(this.getProjector() == null, "projector already set");
		setProjector(p);
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
		Preconditions.checkState(getProjector() != null, "projector not set");
		Preconditions.checkState(region != null, "region not set");
	}

	public ASGScanner buildASGScanner() {
		checkRequiredState();
		return build(ASGScanner.class);
	}

	public AMIScanner buildAMIScanner() {
		checkRequiredState();
		return build(AMIScanner.class);
	}

	public VPCScanner buildVPCScanner() {
		checkRequiredState();
		return build(VPCScanner.class);
	}

	public AccountScanner buildAccountScanner() {
		return build(AccountScanner.class);
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

	public ELBScanner buildELBScanner() {
		return build(ELBScanner.class);
	}

	public SpotFleetRequestScanner buildSpotFleetRequestScanner() {
		checkRequiredState();
		return build(SpotFleetRequestScanner.class);
	}

	@SuppressWarnings("unchecked")
	public <T extends AWSScanner> T build(Class<T> clazz) {
		try {
			this.targetType = (Class<AWSScanner>) clazz;
			return (T) clazz.getConstructor(AWSScannerBuilder.class).newInstance(this);
		} catch (IllegalAccessException | InstantiationException | InvocationTargetException
				| NoSuchMethodException e) {
			throw new IllegalStateException(e);
		}

	}

	@SuppressWarnings("unchecked")
	@Override
	public AWSScanner<AmazonWebServiceClient> build() {
		Preconditions.checkState(targetType != null, "target type not set");
		return (AWSScanner<AmazonWebServiceClient>) build(this.targetType);
	}

	public Region getRegion() {
		return region;
	}

	public boolean isIncludeGlobalResources() {
		return includeGlobalResources;
	}

	/* package */
	AtomicReference<AWSParallelScannerGroup> getInitialScannerGroup() {
		return initialScannerGroup;
	}

	@SuppressWarnings("unchecked")
	@Override
	public AWSScannerBuilder withFailOnError(boolean b) {
		// this aids fluency by setting the correct return type
		return super.withFailOnError(b);
	}

	public AWSScannerBuilder withClientConfiguration(ClientConfiguration clientConfiguration) {
		this.clientConfiguration = clientConfiguration;
		return this;
	}

	public ClientConfiguration getClientConfiguration() {
		return this.clientConfiguration;
	}
	
	public AWSScannerBuilder withConfig(AWSScanner x) {
		return withAccountId(x.getAccountId()).withRegion(x.getRegion()).withClientConfiguration(x.getAWSScannerBuilder().getClientConfiguration()).withCredentials(x.getAWSScannerBuilder().getCredentialsProvider());
	}
}
