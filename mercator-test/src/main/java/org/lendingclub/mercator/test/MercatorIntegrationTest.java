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
package org.lendingclub.mercator.test;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.lendingclub.mercator.core.Projector;

public class MercatorIntegrationTest {

	static  Boolean neo4jAvailable;
	static Projector projector=null;
	
	@BeforeClass
	public static void checkNeo4j() {
		if (neo4jAvailable==null) {
			Projector p = new Projector.Builder().build();
			neo4jAvailable = p.getNeoRxClient().checkConnection();
			projector = p;
		}
		Assume.assumeTrue(neo4jAvailable);
	}
	
	public boolean isNeo4jAvailable() {
		return neo4jAvailable!=null && neo4jAvailable;
	}
	public Projector getProjector() {
		return projector;
	}
	
	@After
	public void deleteMercatorTestData() {
		if (neo4jAvailable!=null && neo4jAvailable.booleanValue()) {
			projector.getNeoRxClient().execCypher("match (a:JUnitTestNode) detach delete a");
		}
	}
}
