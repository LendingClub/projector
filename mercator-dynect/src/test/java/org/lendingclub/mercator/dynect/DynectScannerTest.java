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
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.lendingclub.mercator.core.Projector;

import com.fasterxml.jackson.databind.node.ObjectNode;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;

public class DynectScannerTest {
	
	@Rule
	public MockWebServer mockServer = new MockWebServer();
	
	DynectClient client;

	
	@Test
	@Ignore
	public void scanZoneRecordTest() {
		
		String jsonResponse = getSessionResponseJsonBody();
		mockServer.enqueue(new MockResponse().setBody(jsonResponse));
		
		client = new DynectClient.DynBuilder().withUrl(mockServer.url("/login").toString()).withProperties("LendingClub", "mercator", "test").build();

		DynectScannerBuilder dynBuilder = new DynectScannerBuilder().withProperties("LendingClub", "mercator", "test");
		
		DynectScanner scanner = new DynectScanner(dynBuilder);

		mockServer.enqueue(new MockResponse().setBody(getRecordResponseJsonBody()));
	
		ObjectNode response = client.get("/REST/ARecord/Test.com/api-sandbox.Test.com/126050797");
		ObjectNode data = scanner.toRecordJson(response.get("data"));
		
		Assertions.assertThat(data.get("zoneName").asText().equals("Test.com"));
		Assertions.assertThat(data.get("address").asText().equals("0.0.0.0"));
		Assertions.assertThat(data.get("recordName").asText().equals("api-sandbox.Test.com"));
		Assertions.assertThat(data.get("recordType").asText().equals("A"));
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
	
	private String getRecordResponseJsonBody() {
		String body = "{"
		        + " \"status\": \"success\","
		        + " \"data\": {"
		        + " \"zone\": \"Test.com\", "
		        + " \"ttl\": 300, "
		        + " \"fqdn\": \"api-sandbox.Test.com\", "
		        + " \"record_type\": \"A\", "
		        + " \"rdata\": { "
		        + " \"address\": \"0.0.0.0\" },"
		        + " \"version\": \"3.7.5\" "
		        + "  }"
		        + "}";
		return body;
	}
}
