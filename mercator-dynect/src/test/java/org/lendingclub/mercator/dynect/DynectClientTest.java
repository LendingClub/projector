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
package org.lendingclub.mercator.dynect;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;

public class DynectClientTest {
	
	@Rule
	public MockWebServer mockServer = new MockWebServer();
	
	DynectClient client;
	
	@Before
	public void setup() {
		String jsonResponse = getSessionResponseJsonBody();
		mockServer.enqueue(new MockResponse().setBody(jsonResponse));
		client = new DynectClient.DynBuilder().withUrl(mockServer.url("/login").toString()).withProperties("LendingClub", "mercator", "test").build();
		Assertions.assertThat(client.getToken().equals("9IlKZpX0ZAQzsq3kiXJCiFGJVs7EB2ZouvMcwJORo8"));
	}
	
	@Test
	public void allZoneTest() {
		
		mockServer.enqueue(new MockResponse().setBody(getZoneResponseJsonBody()));
		
		ObjectNode zonesData = client.get("/REST/Zone");
		Assertions.assertThat(zonesData.get("status").toString().equals("success"));
		Assertions.assertThat(zonesData.get("data").isArray());
		Assertions.assertThat(zonesData.get("data").size() == 2);
	}
	
	@Test
	public void zoneTest() {
		
		mockServer.enqueue(new MockResponse().setBody(zoneResponseBody()));
		ObjectNode zonesData = client.get("/REST/Zone");
		Assertions.assertThat(zonesData.get("data").get("zone").asText().equals("LendingClub.com"));
	}	
		
	private String getSessionResponseJsonBody() {
		String body = "{"
		        + " \"status\": \"success\","
		        + " \"data\": {"
		        + " \"token\": \"9IlKZpX0ZAQzsq3kiXJCiFGJVs7EB2ZouvMcwJORo8\", "
		        + " \"version\": \"3.7.5\" "
		        + "  }"
		        + "}";
		return body;
	}
	
	private String getZoneResponseJsonBody() {
		String body = "{"
				+ "\"status\": \"success\", "
				+ "\"data\": [\"/REST/Zone/LENDINGCLUB.COM\", \"/REST/Zone/Test.com\"]"
				+ "}";
		return body;
	}
	
	private String zoneResponseBody() {
		String body = "{"
				+ "\"status\":\"success\","
				+ "\"data\": { "
				+ "\"zone_type\":\"primary\", "
				+ "\"serial_style\": \"increment\", "
				+ "\"zone\":\"LendingClub.com\"}"
				+ "}";
		return body;
	}

}
