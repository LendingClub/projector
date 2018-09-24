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

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.lendingclub.neorx.NeoRxClient;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.pricing.AWSPricingClient;
import com.amazonaws.services.pricing.model.Filter;
import com.amazonaws.services.pricing.model.FilterType;
import com.amazonaws.services.pricing.model.GetProductsRequest;
import com.amazonaws.services.pricing.model.GetProductsResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Strings;

/**
 * AWS offers up all its products and prices in a single pricing file, but it's
 * ludicrously lage and nearly all of it is useless to any one customer. So
 * instead we'll use the pricing API and fetch the data on-demand (caching it in
 * neo4j for a day).
 * 
 * The pricing API suffers from being very general; this class is focused on
 * giving prices for EC2 instances only.
 */
public class AwsComputeInstancePricer {

	/**
	 * These little classes are constants from the data-dictionary and provide a
	 * mechanism to find the canonical form. (AWS accepts both "3yr" and "3 yr" for
	 * example, but returns "3yr". Also, the EC2 API's use similar but not exactly
	 * the same values for the same thing, these classes do some of the
	 * translation.)
	 * 
	 * Fetched via:
	 * 
	 * aws pricing get-attribute-values --service-code AmazonEC2 --region us-east-1
	 * --query AttributeValues[].Value --attribute-name operatingSystem
	 * 
	 * @see AwsComputeInstancePricer#canon(Class, String)
	 */
	public static abstract class DD_TERM {
		public String canon(String s) {
			Field[] fields = getClass().getDeclaredFields();
			for (int i = 0; i < fields.length; i++) {
				int m = fields[i].getModifiers();
				if (Modifier.isPublic(m) && Modifier.isStatic(m) && fields[i].getType().equals(String.class)) {
					try {
						String v = (String) fields[i].get(null);
						if (s.equalsIgnoreCase(v)) {
							return v;
						}
					} catch (IllegalArgumentException | IllegalAccessException e) {
					}
				}
			}
			return s;
		}
	}

	public static class OPERATING_SYSTEM extends DD_TERM {
		public static final String Generic = "Generic", Linux = "Linux", NA = "NA", RHEL = "RHEL", SUSE = "SUSE",
				Windows = "Windows";

		@Override
		public String canon(String s) {
			return Strings.isNullOrEmpty(s) ? Linux : super.canon(s);
		}
	}

	public static class OFFERING_CLASS extends DD_TERM {
		public static final String convertible = "convertible", standard = "standard";
	}

	public static class PURCHASE_OPTION extends DD_TERM {
		public static final String All_Upfront = "All Upfront", No_Upfront = "No Upfront",
				Partial_Upfront = "Partial Upfront";
	}

	public static class TENANCY extends DD_TERM {
		public static final String Dedicated = "Dedicated", Host = "Host", NA = "NA", Reserved = "Reserved",
				Shared = "Shared";

		@Override
		public String canon(String s) {
			return (Strings.isNullOrEmpty(s) || "default".equals(s)) ? Shared : super.canon(s);
		}
	}

	public static class TERM_TYPE extends DD_TERM {
		public static final String OnDemand = "OnDemand", Reserved = "Reserved";
	}

	public static class LEASE_CONTRACT_LENGTH extends DD_TERM {
		public static final String _1yr = "1yr", _3yr = "3yr";
	}

	public static class PriceDetail {
		public PriceDetail(double fixedPrice, double hourlyRate) {
			this.fixedPrice = fixedPrice;
			this.hourlyRate = hourlyRate;
		}

		public double fixedPrice;
		public double hourlyRate;
	}

	private static final String PRODUCT_CLAUSE = "(n:AwsComputeInstanceProduct { "
			+ " aws_region: {region}, aws_operatingSystem: {operatingSystem}, aws_instanceType: {instanceType}, aws_tenancy: {tenancy} })";
	private static Map<Regions, String> regionsToLocationsName = new HashMap<>();;
	static {
		/*
		 * The pricing API uses long region names, which don't appear to be available
		 * elsewhere programmatically. So build it ourselves.
		 */
		Object[] values = new Object[] { Regions.GovCloud, "AWS GovCloud (US)", Regions.AP_SOUTH_1,
				"Asia Pacific (Mumbai)", Regions.AP_NORTHEAST_2, "Asia Pacific (Seoul)", Regions.AP_NORTHEAST_1,
				"Asia Pacific (Singapore)", Regions.AP_SOUTHEAST_2, "Asia Pacific (Sydney)", Regions.AP_SOUTHEAST_1,
				"Asia Pacific (Tokyo)", Regions.CA_CENTRAL_1, "Canada (Central)", Regions.EU_CENTRAL_1,
				"EU (Frankfurt)", Regions.EU_WEST_1, "EU (Ireland)", Regions.EU_WEST_2, "EU (London)",
				// Regions.EU_WEST_3, "EU (Paris)",
				Regions.SA_EAST_1, "South America (Sao Paulo)", Regions.US_EAST_1, "US East (N. Virginia)",
				Regions.US_EAST_2, "US East (Ohio)", Regions.US_WEST_1, "US West (N. California)", Regions.US_WEST_2,
				"US West (Oregon)" };
		for (int i = 0; i < values.length; i += 2) {
			regionsToLocationsName.put((Regions) values[i], (String) values[i + 1]);
		}

	}
	private static final ObjectMapper mapper = new ObjectMapper();
	private AWSPricingClient pricing;
	private NeoRxClient neo4j;
	private JsonConverter jsonConverter = new JsonConverter().withFlattenNestedObjects(true);

	public AwsComputeInstancePricer(AWSPricingClient pricing, NeoRxClient neo4j) {
		this.pricing = pricing;
		this.neo4j = neo4j;
	}

	/**
	 * Find the canonical value of a term.
	 */
	public static String canon(Class<? extends DD_TERM> clss, String s) {
		try {
			return clss.newInstance().canon(s);
		} catch (InstantiationException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Return the on demand EC2 instance rate, in USD/hr.
	 */
	public double getOnDemandRate(String regionName, String operatingSystem, String instanceType, String tenancy) {
		operatingSystem = canon(OPERATING_SYSTEM.class, operatingSystem);
		tenancy = canon(TENANCY.class, tenancy);
		refreshProduct(regionName, operatingSystem, instanceType, tenancy);
		String cypher = "match " + PRODUCT_CLAUSE + "-[:ON_DEMAND]-(p:AwsPriceSchedule) return p";
		JsonNode p = neo4j.execCypher(cypher, "region", regionName, "operatingSystem", operatingSystem, "instanceType",
				instanceType, "tenancy", tenancy).blockingFirst(MissingNode.getInstance());
		if (p.isMissingNode()) {
			throw new IllegalArgumentException("cannot find on-demand rate for "
					+ Arrays.asList(regionName, operatingSystem, instanceType, tenancy));
		}
		return p.path("aws_hourlyRate").asDouble();
	}

	/**
	 * Return the fixed price and hourly rate of a RI contract.
	 */
	public PriceDetail getReservedInstancePrice(String regionName, String operatingSystem, String instanceType,
			String tenancy, String term, String offeringClass, String purchaseOption) {
		operatingSystem = canon(OPERATING_SYSTEM.class, operatingSystem);
		tenancy = canon(TENANCY.class, tenancy);
		term = canon(LEASE_CONTRACT_LENGTH.class, term);
		offeringClass = canon(OFFERING_CLASS.class, offeringClass);
		purchaseOption = canon(PURCHASE_OPTION.class, purchaseOption);
		refreshProduct(regionName, operatingSystem, instanceType, tenancy);
		String cypher = "match " + PRODUCT_CLAUSE + "-[:RESERVED]->(p:AwsPriceSchedule {"
				+ " aws_termAttributes_leaseContractLength: {term}, aws_termAttributes_offeringClass: {offeringClass},"
				+ " aws_termAttributes_purchaseOption: {purchaseOption} }) return p";
		JsonNode p = neo4j.execCypher(cypher, "region", regionName, "operatingSystem", operatingSystem, "instanceType",
				instanceType, "tenancy", tenancy, "term", term, "offeringClass", offeringClass, "purchaseOption",
				purchaseOption).blockingFirst(MissingNode.getInstance());
		if (p.isMissingNode()) {
			throw new IllegalArgumentException("cannot find reserved instance price for " + Arrays.asList(regionName,
					operatingSystem, instanceType, tenancy, term, offeringClass, purchaseOption));
		}
		return new PriceDetail(p.path("aws_fixedPrice").asDouble(), p.path("aws_hourlyRate").asDouble());
	}

	/**
	 * Return AWS EC2 product information.
	 */
	public JsonNode getProduct(String regionName, String operatingSystem, String instanceType, String tenancy) {
		operatingSystem = canon(OPERATING_SYSTEM.class, operatingSystem);
		tenancy = canon(TENANCY.class, tenancy);
		refreshProduct(regionName, operatingSystem, instanceType, tenancy);
		String cypher = "match " + PRODUCT_CLAUSE + " return n";
		return neo4j.execCypher(cypher, "region", regionName, "operatingSystem", operatingSystem, "instanceType",
				instanceType, "tenancy", tenancy).blockingFirst(MissingNode.getInstance());
	}

	private void refreshProduct(String regionName, String operatingSystem, String instanceType, String tenancy) {
		long t = neo4j
				.execCypher("match " + PRODUCT_CLAUSE + " return n.updateTs", "region", regionName, "operatingSystem",
						operatingSystem, "instanceType", instanceType, "tenancy", tenancy)
				.blockingFirst(MissingNode.getInstance()).asLong();
		// prices good for a day at least
		if (t < (System.currentTimeMillis() - TimeUnit.HOURS.toMillis(24L))) {
			fetchProduct(regionName, operatingSystem, instanceType, tenancy);
		}
	}

	private JsonNode fetchProduct(String regionName, String operatingSystem, String instanceType, String tenancy) {
		Regions region = Regions.fromName(regionName);

		GetProductsRequest request = new GetProductsRequest().withServiceCode("AmazonEC2").withFilters(
				filter("productFamily", "Compute Instance"), filter("location", regionsToLocationsName.get(region)),
				filter("operatingSystem", operatingSystem), filter("instanceType", instanceType),
				filter("tenancy", tenancy));
		GetProductsResult products = pricing.getProducts(request);

		if (products.getPriceList().isEmpty()) {
			return MissingNode.getInstance();
		}
		/*
		 * The price list is embedded json. There may be more than one price list: for
		 * Windows there are options for SQL Server (preInstalledSw: SQL Web) and
		 * licenceModel (no license, byol). We're just going to record the first one we
		 * find with no preInstalledSw (actual value NA).
		 */
		JsonNode priceList = null;
		for (String priceListText : products.getPriceList()) {
			JsonNode n;
			try {
				n = mapper.readTree(priceListText);
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
			String preInstalledSw = n.path("product").path("attributes").path("preInstalledSw").asText();
			if (preInstalledSw.equals("") || preInstalledSw.equals("NA")) {
				priceList = n;
				break;
			}
		}
		if (priceList == null) {
			return MissingNode.getInstance();
		}

		/*
		 * The price list describes both the product, and one or more prices under
		 * different terms (on demand, dozen or so RI combinations.) Store the product
		 * under AwsComputeInstanceProduct and relate it to AwsPriceSchedule nodes.
		 * 
		 * Note that "product" is slightly weird in that there's not a single "c4.large"
		 * product, but instead a different 'c4.large' product for each combination of
		 * region, operating system, and tenancy.
		 */
		ObjectNode product = (ObjectNode) priceList.path("product");
		ObjectNode productAttributes = (ObjectNode) product.path("attributes");

		ObjectNode props = jsonConverter.toJson(productAttributes);
		props.put("aws_sku", product.path("sku").asText());

		neo4j.execCypher("merge " + PRODUCT_CLAUSE + " set n += {props}, n.updateTs = {t}", "region", regionName,
				"operatingSystem", operatingSystem, "instanceType", instanceType, "tenancy", tenancy, "props", props,
				"t", System.currentTimeMillis());

		neo4j.execCypher("match " + PRODUCT_CLAUSE + "--(p:AwsPriceSchedule) detach delete p return count(p)", "region",
				regionName, "operatingSystem", operatingSystem, "instanceType", instanceType, "tenancy", tenancy)
				.blockingFirst().asInt();

		JsonNode onDemand = priceList.path("terms").path(TERM_TYPE.OnDemand);
		if (onDemand.isObject()) {
			savePrices(regionName, operatingSystem, instanceType, tenancy, "ON_DEMAND", onDemand);
		}

		JsonNode reserved = priceList.path("terms").path(TERM_TYPE.Reserved);
		if (reserved.isObject()) {
			savePrices(regionName, operatingSystem, instanceType, tenancy, "RESERVED", reserved);
		}

		return priceList;

	}

	private void savePrices(String regionName, String operatingSystem, String instanceType, String tenancy,
			String label, JsonNode p) {
		for (Iterator<String> iter = p.fieldNames(); iter.hasNext();) {
			String pricingCode = iter.next();
			JsonNode pricingNode = p.get(pricingCode);
			ObjectNode n = pricingNode.deepCopy();
			n.remove("priceDimensions");
			n = jsonConverter.toJson(n);
			/*
			 * Untangle the priceDimensions structure to pull out the hourly rate and fixed
			 * price.
			 */
			JsonNode priceDimensionsNode = pricingNode.path("priceDimensions");
			for (Iterator<String> dimensionIter = priceDimensionsNode.fieldNames(); dimensionIter.hasNext();) {
				String dimensionName = dimensionIter.next();
				JsonNode dimensionNode = priceDimensionsNode.get(dimensionName);
				String pricePerUnit = dimensionNode.path("pricePerUnit").path("USD").asText();
				if (dimensionNode.path("unit").asText().equals("Hrs")) {
					n.put("aws_hourlyRate", pricePerUnit);
				} else if (dimensionNode.path("description").asText().equals("Upfront Fee")) {
					n.put("aws_fixedPrice", pricePerUnit);
				}
			}
			String cypher = "match " + PRODUCT_CLAUSE + " create (n)-[:" + label
					+ "]->(p:AwsPriceSchedule) set p += {props}";
			neo4j.execCypher(cypher, "region", regionName, "operatingSystem", operatingSystem, "instanceType",
					instanceType, "tenancy", tenancy, "props", n);
		}
	}

	private Filter filter(String field, String value) {
		return new Filter().withType(FilterType.TERM_MATCH).withField(field).withValue(value);
	}

}
