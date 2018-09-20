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
package org.lendingclub.mercator.dynect;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import io.macgyver.okrest3.OkHttpClientConfigurer;
import io.macgyver.okrest3.OkRestClient;
import io.macgyver.okrest3.OkRestClientConfigurer;
import io.macgyver.okrest3.OkRestTarget;

public class DynectClient {
	
	private static final String ENDPOINT_URL = "https://api2.dynect.net/";
	private static Logger logger = LoggerFactory.getLogger(DynectClient.class);
	private OkRestTarget restTarget;
	
	private String username;
	private String password;
	private String customerName;
	private Token token;
	
	private AtomicReference<Token> tokenRef = new AtomicReference<>();
	private TokenSupplier tokenSupplier = new TokenSupplier();
	
	public String getToken() {
		token = tokenRef.get();
		
		if ( token == null || token.isExpired(5, TimeUnit.MINUTES)) {
			token = tokenSupplier.get();
		} else {
			logger.debug(" Dyn token is still valid");
		}
		tokenRef.set(token);
		return token.getValue();
	}
	
	public class Token {
		String value;
		int refreshPeriod = 300;
		long refreshTime = System.currentTimeMillis();
		
		public String getValue() {
			return value;
		}
		
		public int getRefreshPeriod() {
			return refreshPeriod;
		}
		
		public boolean isExpired(long time, TimeUnit unit) {
			return System.currentTimeMillis() - refreshTime > unit.toMillis(time);
		}
	}
	
	public static class DynBuilder {
		String url = ENDPOINT_URL;
		String username;
		String password;
		String customerName;
		
		io.macgyver.okrest3.OkRestClient.Builder builder;
		
		OkRestClientConfigurer okRestConfigurer;
		OkHttpClientConfigurer okHttpConfigurator;
		
		public DynBuilder() {
			builder = new OkRestClient.Builder();
		}
		
		public DynBuilder withUrl(String url) {
			this.url = url;
			return this;
		}
		
		public DynBuilder withProperties(String customerName, String username, String password) {
			this.customerName = customerName;
			this.username = username;
			this.password = password;
			return this;
		}

		public DynBuilder withConfig(OkRestClientConfigurer okRestConfig) {
			this.okRestConfigurer = okRestConfig;
			return this;
		}
		
		public DynectClient build() {
			
			DynectClient client = new DynectClient();
			
			if (okRestConfigurer != null) {
				builder.withOkRestClientConfig(okRestConfigurer);
			}
			if (okHttpConfigurator != null) {
				builder.withOkHttpClientConfig(okHttpConfigurator);
			}

			logger.info("constructing DynectClient for {}",url);

			client.restTarget = builder.build().uri(url);
			client.username = username;
			client.password = password;
			client.customerName = customerName;
			return client;
		}
	}
	
	class TokenSupplier implements Supplier<Token> {

		@Override
		public Token get() {
			logger.info(" Generating token for Dynect Client using url {} ", restTarget.getUrl());
			
			ObjectMapper mapper = new ObjectMapper();
			ObjectNode data = mapper.createObjectNode();
			
			data.put("customer_name", customerName).put("user_name", username).put("password", password);
					
			ObjectNode response = restTarget.path("/REST/Session/").addHeader("Content-Type", "application/json").post(data).execute(ObjectNode.class);
			Token token = new Token();
			
			if ( response != null) {
				token.value = response.get("data").get("token").asText();
				if (Strings.isNullOrEmpty(token.value)) {
					throw new DynectException("Failed to authenticate.");
				}
			}
			
			logger.info("Successfully authenticated with Dynect using {} url", restTarget.getUrl());
			return token;	
		}
		
	}
	
	public OkRestTarget getRestTarget() {
		return restTarget;
	}
	
	public ObjectNode get(String path) { 
		Preconditions.checkArgument(path != null || path != "",
				"path can not be empty");
		ObjectNode response = getRestTarget().path(path).header("Auth-Token", getToken()).addHeader("Content-Type", "application/json").get().execute(ObjectNode.class);
		return response;
	}
}
