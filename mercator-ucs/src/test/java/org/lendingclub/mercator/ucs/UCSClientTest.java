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
package org.lendingclub.mercator.ucs;

import java.io.IOException;
import java.io.StringReader;
import java.util.UUID;

import org.assertj.core.api.Assertions;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.lendingclub.mercator.ucs.UCSClient;
import org.lendingclub.mercator.ucs.UCSClient.Token;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

public class UCSClientTest {

	@Rule
	public MockWebServer mockServer = new MockWebServer();

	UCSClient ucs;

	String cookie;
	
	Document parseXml(RecordedRequest rr) throws IOException,JDOMException{
		return parseXml(rr.getBody().readUtf8());
	}
	Document parseXml(String input) throws IOException,JDOMException {
		SAXBuilder b = new SAXBuilder();
		return b.build(new StringReader(input));
	}
	@Before
	public void setup() {
		cookie = "1234567890/"+UUID.randomUUID().toString();
		mockServer.enqueue(new MockResponse().setBody("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
				+ "<aaaLogin cookie=\"\" response=\"yes\" outCookie=\""+cookie+"\" outRefreshPeriod=\"600\" outPriv=\"read-only\" outDomains=\"\" outChannel=\"noencssl\" outEvtChannel=\"noencssl\" outSessionId=\"web_12345_B\" outVersion=\"3.1(1g)\" outName=\"scott\" />\n"
				+ ""));
		ucs = new UCSClient.Builder().withUrl(mockServer.url("/nuova").toString()).withUsername("scott")
				.withPassword("tiger").build();
	}

	@Test
	public void testLoginLogout() throws InterruptedException, IOException, JDOMException {
		
		mockServer.enqueue(new MockResponse().setBody("<aaaLogout cookie=\"\" response=\"yes\" outStatus=\"success\" />"));
		ucs = new UCSClient.Builder().withUrl(mockServer.url("/nuova").toString()).withUsername("scott")
				.withPassword("tiger").build();
		
		Token token = ucs.getToken();
		Assertions.assertThat(token.getTokenValue()).isEqualTo(cookie);
		RecordedRequest rr = mockServer.takeRequest();
		
		Assertions.assertThat(ucs.getToken()).isSameAs(token);
		Assertions.assertThat(rr.getRequestLine()).startsWith("POST /nuova HTTP/1.1");
		
		Document d = parseXml(rr);
		
		Assertions.assertThat(d.getRootElement().getName()).isEqualTo("aaaLogin");
		Assertions.assertThat(d.getRootElement().getAttributeValue("inName")).isEqualTo("scott");
		Assertions.assertThat(d.getRootElement().getAttributeValue("inPassword")).isEqualTo("tiger");
		
		Assertions.assertThat(ucs.getToken().getTokenValue()).isEqualTo(cookie);
		Assertions.assertThat(ucs.getToken().getRefreshPeriod()).isEqualTo(600);
		
		
		ucs.logout();
		this.cookie = null;
		
		String newCookie = "1234567890/"+UUID.randomUUID().toString();
		mockServer.enqueue(new MockResponse().setBody("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
				+ "<aaaLogin cookie=\"\" response=\"yes\" outCookie=\""+newCookie+"\" outRefreshPeriod=\"600\" outPriv=\"read-only\" outDomains=\"\" outChannel=\"noencssl\" outEvtChannel=\"noencssl\" outSessionId=\"web_12345_B\" outVersion=\"3.1(1g)\" outName=\"scott\" />\n"
				+ ""));
		token = ucs.getToken();
		Assertions.assertThat(ucs.getToken().getTokenValue()).isEqualTo(newCookie);
		
		Assertions.assertThat(ucs.getToken()).isSameAs(token);
		
	}
	@Test
	public void testDummyExchange() {
		mockServer.enqueue(new MockResponse().setBody("<foo response=\"yes\"> </foo>"));
		Element element = ucs.exec("foo", "fizz","buzz");
		
		Assertions.assertThat(element.getName()).isEqualTo("foo");
		Assertions.assertThat(element.getAttributeValue("response")).isEqualTo("yes");
	}
}
