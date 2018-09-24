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
package org.lendingclub.mercator.demo;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.lendingclub.mercator.aws.AwsComputeInstancePricer;
import org.lendingclub.mercator.aws.MultiAccountRegionEntityScanner;
import org.lendingclub.mercator.core.Projector;
import org.lendingclub.mercator.docker.DockerScannerBuilder;
import org.lendingclub.neorx.NeoRxClient;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.pricing.AWSPricingClient;
import com.amazonaws.services.pricing.AWSPricingClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityResult;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;

public class Main {

	public static void main(String[] args) {
		SLF4JBridgeHandler.removeHandlersForRootLogger();
		SLF4JBridgeHandler.install();

		AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
		Consumer<AWSCredentialsProvider> action = Main::scan;
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-role") && i + 1 < args.length) {
				String roleName = args[++i];
				AWSSecurityTokenService sts = AWSSecurityTokenServiceClientBuilder.standard().build();
				credentialsProvider = new STSAssumeRoleSessionCredentialsProvider.Builder(roleName,
						"mercator-demo-" + System.getProperty("user.name")).withStsClient(sts).build();
			} else if (args[i].equals("-price") && i + 4 < args.length) {
				final int fi = i;
				i += 4;
				action = (c) -> {
					getPrice(c, args[fi + 1], args[fi + 2], args[fi + 3], args[fi + 4]);
				};

			}
		}
		action.accept(credentialsProvider);
	}

	private static void getPrice(AWSCredentialsProvider credentialsProvider, String regionName, String platform,
			String instanceType, String tenancy) {
		// the pricing API is only available in us-east-1 and ap-south-1
		AWSPricingClient pricing = (AWSPricingClient) AWSPricingClientBuilder.standard().withRegion(Regions.US_EAST_1)
				.withCredentials(credentialsProvider).build();
		NeoRxClient neo4j = new Projector.Builder().build().getNeoRxClient();
		Double n = new AwsComputeInstancePricer(pricing, neo4j).getOnDemandRate(regionName, platform,
				instanceType, tenancy);
		System.out.println(n);
	}

	private static void scan(AWSCredentialsProvider credentialsProvider) {

		Projector projector = new Projector.Builder().build();

		List<Regions> regionList = Splitter.on(",").omitEmptyStrings().trimResults()
				.splitToList(Strings.nullToEmpty(System.getenv("AWS_REGIONS"))).stream().map(r -> Regions.fromName(r))
				.collect(Collectors.toList());

		AWSSecurityTokenService sts = AWSSecurityTokenServiceClientBuilder.standard()
				.withCredentials(credentialsProvider).build();
		GetCallerIdentityResult awsIdentity = sts.getCallerIdentity(new GetCallerIdentityRequest());

		if (regionList.isEmpty()) {
			regionList = Arrays.asList(Regions.US_WEST_2, Regions.US_EAST_1);
		}
		LoggerFactory.getLogger(Main.class).info("scanning regions: {}", regionList);

		MultiAccountRegionEntityScanner awsScanner = projector
				.createBuilder(MultiAccountRegionEntityScanner.Builder.class)
				.withAccountAndRegions(awsIdentity.getAccount(), credentialsProvider, regionList)
				.withGlobalRegion(Regions.US_EAST_1).build();

		Runnable awsTask = new Runnable() {

			public void run() {
				try {
					awsScanner.scan();
				} catch (Exception e) {
					LoggerFactory.getLogger(Main.class).warn("problem", e);
				}
			}

		};

		Runnable dockerTask = new Runnable() {
			public void run() {

				projector.createBuilder(DockerScannerBuilder.class).withLocalDockerDaemon().build().scan();
			}
		};

		ScheduledExecutorService exec = Executors.newScheduledThreadPool(5);
		if (!Boolean.getBoolean("skipAwsScaning")) {
			exec.scheduleWithFixedDelay(awsTask, 0, 1, TimeUnit.MINUTES);
		}
		if (!Boolean.getBoolean("skipDockerScanning")) {
			exec.scheduleWithFixedDelay(dockerTask, 0, 10, TimeUnit.SECONDS);
		}

		while (true) {
			try {
				Thread.sleep(1000);
			} catch (Exception e) {
			}
		}
	}

}
