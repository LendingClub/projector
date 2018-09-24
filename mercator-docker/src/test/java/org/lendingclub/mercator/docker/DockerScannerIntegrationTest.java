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
package org.lendingclub.mercator.docker;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.lendingclub.mercator.core.NotFoundException;

public class DockerScannerIntegrationTest extends LocalDockerDaemonIntegrationTest {

	@Test
	public void testIt2() {
		getDockerScanner().scan();

	}
	
	@Test
	public void testUnnownTask() {
		getDockerScanner().scanTask("foobar");
	}
	@Test
	public void testTargetServiceScan() {
		getDockerScanner().scanService("foo");
	}

	@Test
	public void testNotFoundException() {
		try {
			getDockerScanner().getRestClient().get("/nodes/nodenotofound");
			Assertions.failBecauseExceptionWasNotThrown(NotFoundException.class);
		} 
		catch (Exception e) {
			if (e.toString().contains("not a swarm manager")) {
				return;
			}
			Assertions.assertThat(e).isInstanceOf(NotFoundException.class);
		}
	}

	@Test
	public void testIt() {
		getDockerScanner().getSchemaManager().applyConstraints();
		// this will not run if neo4j or docker is not available
		getDockerScanner().scan();
	}
	
	@Test
	public void testSwarmScanner() {
		getDockerScanner().scanService("objective_lewinx");
	}
}
