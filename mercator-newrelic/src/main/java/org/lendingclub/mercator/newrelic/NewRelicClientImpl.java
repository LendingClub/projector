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

package org.lendingclub.mercator.newrelic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.macgyver.okrest3.OkRestTarget;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;
import io.macgyver.okrest3.OkRestClient.Builder;

public class NewRelicClientImpl implements NewRelicClient {
	
	private OkRestTarget restTarget;
	private Logger logger = LoggerFactory.getLogger(NewRelicClientImpl.class);
	private ObjectMapper mapper = new ObjectMapper();
	private String accountId;

	public String getAccountId() {
		return accountId;
	}

	public class ApiKeyInterceptor implements Interceptor {
		
		String apiKey;
		
		public ApiKeyInterceptor(String key) {
			this.apiKey = key;
		}

		@Override
		public Response intercept(Chain chain) throws IOException {
			
			Request request = chain.request().newBuilder().addHeader("X-Api-Key", apiKey).build();
			return chain.proceed(request);
		}
		
	}
	
	public NewRelicClientImpl( String token , String accountId) {
		
		this.accountId = accountId;
		
		Builder builder = new Builder();
		
		if(!Strings.isNullOrEmpty(token)) {
			builder.withOkHttpClientConfig(cfg -> {
				cfg.addInterceptor(this.new ApiKeyInterceptor(token));
			});
			this.restTarget = builder.build().uri(ENDPOINT_URL);
			
		} else {
			logger.error("NewRelic token is either empty or null");
		}
	}


	@Override
	public OkRestTarget getOkRestTarget() {
		return restTarget.path("v2");
	}


	@Override
	public ObjectNode get(String path, String... args) {
		Preconditions.checkArgument(path != null || path != "",
				"path can not be empty");
		OkRestTarget localTarget = getOkRestTarget().path(path);
		localTarget = localTarget.queryParam((Object[]) args).accept(MEDIA_TYPE_APPLICATION_JSON).contentType(APPLICATION_FORM_URLENCODED_VALUE);
		return localTarget.get().execute(ObjectNode.class);
	}


	@Override
	public ObjectNode getApplications() {
		ObjectNode appsNode = mapper.createObjectNode();
		List<JsonNode> applicationsList = new ArrayList<JsonNode>();	
		int page = 1;
		JsonNode apps = getApplications(page);
		while(apps.size() != 0) {
			List<JsonNode> tempAppsList = convertJsonNodeToList(apps);	
			applicationsList.addAll(tempAppsList);
			page++;
			apps = getApplications(page);
		}
		appsNode.put("applications", mapper.valueToTree(applicationsList));
		return appsNode;
	}

	@Override
	public ObjectNode getServers() {
		ObjectNode serversNode = mapper.createObjectNode();
		List<JsonNode> serversList = new ArrayList<JsonNode>();
		int page = 1;
		JsonNode servers = getServers(page);
		while(servers.size() != 0) {
			List<JsonNode> tempServersList = convertJsonNodeToList(servers);	
			serversList.addAll(tempServersList);
			page++;
			servers = getServers(page);
		}
		serversNode.put("servers", mapper.valueToTree(serversList));
		return serversNode;
	}

	@Override
	public ObjectNode getAlertPolicies() {
		ObjectNode alertPoliciesNode = mapper.createObjectNode();
		List<JsonNode> alertPoliciesList = new ArrayList<JsonNode>();
		int page = 1;
		JsonNode policies = getAlertPolicies(page);
		while(policies.size() != 0) {
			List<JsonNode> tempPoliciesList = convertJsonNodeToList(policies);	
			alertPoliciesList.addAll(tempPoliciesList);
			page++;
			policies = getAlertPolicies(page);
		}
		alertPoliciesNode.put("policies", mapper.valueToTree(alertPoliciesList));
		return alertPoliciesNode;
	}
	
	@Override
	public ObjectNode getAlertConditionForPolicy(String policyId) {
		ObjectNode alertConditionsNode = mapper.createObjectNode();
		List<JsonNode> alertConditionsList = new ArrayList<JsonNode>();
		int page = 1;
		JsonNode conditions = getAlertConditionByPolicy(policyId, page);
		while(conditions.size() != 0) {
			List<JsonNode> tempPoliciesList = convertJsonNodeToList(conditions);	
			alertConditionsList.addAll(tempPoliciesList);
			page++;
			conditions = getAlertConditionByPolicy(policyId, page);
		}
		alertConditionsNode.put("conditions", mapper.valueToTree(alertConditionsList));
		return alertConditionsNode;
	}
	
	private JsonNode getApplications(int page) {
		JsonNode applications = get("applications.json", "page", Integer.toString(page))
		        .path("applications");
		return applications;
	}
	
	private JsonNode getServers(int page) {
		JsonNode servers = get("servers.json", "page", Integer.toString(page)).path("servers");
		return servers;
	}
	
	private JsonNode getAlertPolicies(int page) {
		JsonNode policies = get("alerts_policies.json", "page", Integer.toString(page)).path("policies");
		return policies;
	}
	
	private JsonNode getAlertConditionByPolicy(String policyId, int page) {
		JsonNode conditions = get("alerts_conditions.json", "page", Integer.toString(page), "policy_id", policyId).path("conditions");
        return conditions;
	}
	
	private List<JsonNode> convertJsonNodeToList(JsonNode jsonNode) {
		List<JsonNode> tempList = new ArrayList<JsonNode>();
		TypeReference<List<JsonNode>> typeRef = new TypeReference<List<JsonNode>>() {
		};
		try {
			tempList = mapper.readValue(jsonNode.traverse(), typeRef);
		} catch (IOException e) {
			logger.error("Failed to parse server details from the newrelic response.");
		}
		return tempList;
	}
}
