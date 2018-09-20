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
package org.lendingclub.mercator.core;

import org.assertj.core.api.Assertions;
import org.assertj.core.data.Offset;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;

public class ScanControlTest {

	Logger logger = LoggerFactory.getLogger(ScanControlTest.class);
	Projector projector;

	@Before
	public void setup() {
		try {
			Projector p = new Projector.Builder().build();
			if (p.getNeoRxClient().checkConnection()) {
				this.projector = p;
			}
		} catch (Exception e) {
			logger.warn("could not run integration test");
		}
		Assume.assumeTrue(projector != null);
	}

	@Test
	public void testIt() {
		projector.getNeoRxClient().execCypher("match (a:MercatorScan) detach delete a");
		ScanControl c = new ScanControl(projector, "junit");
		c.markLastScan("foo", "bar");
		c.markLastScan("fizz", "buzz");
		c.markLastScan();

		JsonNode n = projector.getNeoRxClient()
				.execCypher("match (a:MercatorScan) where a.type='junit' and a.foo='bar' return a").blockingSingle();
		Assertions.assertThat(n.path("type").asText()).isEqualTo("junit");
		Assertions.assertThat(n.path("foo").asText()).isEqualTo("bar");
		Assertions.assertThat(n.path("lastScanTs").asLong()).isCloseTo(System.currentTimeMillis(),
				Offset.offset(60000L));

		n = projector.getNeoRxClient()
				.execCypher("match (a:MercatorScan) where a.type='junit' and a.fizz='buzz' return a").blockingSingle();
		Assertions.assertThat(n.path("type").asText()).isEqualTo("junit");
		Assertions.assertThat(n.path("fizz").asText()).isEqualTo("buzz");
		Assertions.assertThat(n.path("lastScanTs").asLong()).isCloseTo(System.currentTimeMillis(),
				Offset.offset(60000L));

		c.markForceRescan("foo", "bar");
		n = projector.getNeoRxClient()
				.execCypher("match (a:MercatorScan) where a.type='junit' and a.foo='bar' return a").blockingSingle();
		Assertions.assertThat(n.path("type").asText()).isEqualTo("junit");
		Assertions.assertThat(n.path("foo").asText()).isEqualTo("bar");
		Assertions.assertThat(n.path("lastScanTs").asLong()).isEqualTo(0);
	}
}
