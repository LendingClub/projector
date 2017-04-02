/**
 * Copyright 2017 Lending Club, Inc.
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
import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;

import org.lendingclub.mercator.core.AbstractScanner;
import org.lendingclub.mercator.core.MercatorException;
import org.lendingclub.mercator.core.Projector;
import org.lendingclub.mercator.core.ScannerContext;
import org.lendingclub.mercator.core.SchemaManager;
import org.lendingclub.neorx.NeoRxClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonWebServiceClient;
import com.amazonaws.client.builder.AwsSyncClientBuilder;
import com.amazonaws.metrics.RequestMetricCollector;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

public abstract class AWSScanner<T extends AmazonWebServiceClient> extends AbstractScanner {

	static ObjectMapper mapper = new ObjectMapper();

	protected final Logger logger = LoggerFactory.getLogger(getClass());
	private Projector projector;
	private AmazonWebServiceClient client;
	private Region region;

	String neo4jLabel = null;
	private ScannerMetricCollector metricCollector = new ScannerMetricCollector();
	protected AWSScannerBuilder builder;
	JsonConverter converter;

	Class<? extends AmazonWebServiceClient> clientType;

	ShadowAttributeRemover shadowRemover;
	public static final String AWS_REGION_ATTRIBUTE = "aws_region";
	public static final String AWS_ACCOUNT_ATTRIBUTE = "aws_account";
	public static final String AWS_ARN_ATTRIBUTE = "aws_arn";

	public AWSScanner(AWSScannerBuilder builder, Class<? extends AmazonWebServiceClient> clientType, String label) {
		super(builder, Maps.newHashMap());
		if (builder.getRegion() == null) {
			builder.withRegion(Region.getRegion(Regions.US_EAST_1));
		}
		this.neo4jLabel = label;
		this.clientType = clientType;

		Preconditions.checkNotNull(builder);
		Preconditions.checkNotNull(builder.getRegion());
		Preconditions.checkNotNull(builder.getProjector());

		this.builder = builder;
		this.region = builder.getRegion();
		this.projector = builder.getProjector();

		this.shadowRemover = new ShadowAttributeRemover(getNeoRxClient());

	}

	protected void setNeo4jLabel(String label) {
		this.neo4jLabel = label;
	}
	protected ShadowAttributeRemover getShadowAttributeRemover() {
		return shadowRemover;
	}

	protected <T extends AmazonWebServiceClient> T createClient(Class<T> clazz) {
		try {
			String builderClass = clazz.getName() + "Builder";
			Class<? extends AwsSyncClientBuilder> builderClazz = (Class<? extends AwsSyncClientBuilder>) Class
					.forName(builderClass);
			Method m = builderClazz.getMethod("standard");
			return (T) builder.configure((AwsSyncClientBuilder) m.invoke(null)).build();
		} catch (ClassNotFoundException | IllegalAccessException | InvocationTargetException
				| NoSuchMethodException e) {
			throw new MercatorException(e);
		}

	}

	protected T createClient() {
		return (T) createClient(clientType);
	}

	public Projector getProjector() {
		return projector;
	}

	@SuppressWarnings("unchecked")
	public T getClient() {
		if (this.client == null) {
			this.client = createClient();
		}
		return (T) this.client;
	}

	public Region getRegion() {

		return region;
	}

	public String getAccountId() {
		return (String) builder.getAccountIdSupplier().get();
	}

	public NeoRxClient getNeoRxClient() {
		return projector.getNeoRxClient();
	}

	class AWSScannerContext extends ScannerContext {
		
		@Override
		protected ToStringHelper toStringHelper() {
			ToStringHelper tsh = super.toStringHelper();
			tsh.add("region",getRegion().getName());
			try {
				tsh.add("account", getAccountId());
			}
			catch (RuntimeException e) {
				tsh.add("account", "unknown");
			}
			return tsh;
		}

	
	}

	public final void scan() {

		List<String> x = Splitter.on(".").splitToList(getClass().getName());
		
		new AWSScannerContext().withName(x.get(x.size()-1)).exec(ctx -> {

			try {

				logger.info("{} started scan", toString());

				doScan();
			} catch (RuntimeException e) {
				maybeThrow(e);
			}

		});

	}

	protected abstract void doScan();

	public ObjectNode convertAwsObject(Object x, Region region) {
		ObjectNode n = JsonConverter.newInstance(getAccountId(), region).toJson(x, null);
		Optional<String> arn = computeArn(n);
		if (arn.isPresent()) {
			n.put(AWS_ARN_ATTRIBUTE, arn.get());
		}
		if (region != null) {
			n.put(AWS_REGION_ATTRIBUTE, region.getName());
		}
		n.put(AWS_ACCOUNT_ATTRIBUTE, getAccountId());
		return n;

	}

	public GraphNodeGarbageCollector newGarbageCollector() {
		return new GraphNodeGarbageCollector().neo4j(getNeoRxClient()).account(getAccountId()).region(getRegion()).label(getNeo4jLabel());
	}

	public Optional<String> computeArn(JsonNode n) {

		return Optional.empty();

	}

	@Override
	public SchemaManager getSchemaManager() {
		return new AWSSchemaManager(getNeoRxClient());
	}

	public RequestMetricCollector getMetricCollector() {
		return metricCollector;
	}

	public String toString() {
		String safeAccount = "unknown";
		try {
			safeAccount = getAccountId();
		} catch (Exception e) {
			// getAccountId() could trigger a callout...bad thing to happen from
			// toString()
		}
		return MoreObjects.toStringHelper(this).add(AWS_REGION_ATTRIBUTE, getRegion().getName())
				.add(AWS_ACCOUNT_ATTRIBUTE, safeAccount).toString();
	}

	protected boolean tokenHasNext(String token) {
		return (!Strings.isNullOrEmpty(token)) && (!token.equals("null"));

	}
	
	public String getNeo4jLabel() {
		Preconditions.checkState(neo4jLabel!=null);
		return neo4jLabel;
	}
	
	protected void incrementEntityCount() {
		ScannerContext.getScannerContext().ifPresent(sc -> {
			sc.incrementEntityCount();
		});
	}
}
