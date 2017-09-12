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
package org.lendingclub.mercator.bind;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.lendingclub.mercator.bind.model.ResourceRecordSet;
import org.lendingclub.mercator.core.Projector;

@Ignore
public class BindScannerTest {
	
	BindScanner scanner;
	
	@Before
	public void setup() {
		String dnsServer = "0.0.0.0";
		List<String> zones = Arrays.asList("example.com");
		String keyName = "bindKey";
		String keyValue = "bindKeyValue";
		String keyAlgo = "HMAC_SHA256";	
		scanner = new Projector.Builder().build().createBuilder(BindScannerBuilder.class).withProperties(dnsServer, zones, keyName, keyValue, keyAlgo).build();

	}
	
	@Test
	public void test() {
		
		scanner.scan();	
	}
	
	@Test
	public void getRecordTest() {
		
		String line = "test.example.com. 60 IN	A	10.10.10.10";	
		scanner = new BindScanner(null);
		ResourceRecordSet<Map<String, Object>> dnsRecord = scanner.getRecord(line);
		Assert.assertEquals(dnsRecord.getName(), "test.example.com");
	}
}
