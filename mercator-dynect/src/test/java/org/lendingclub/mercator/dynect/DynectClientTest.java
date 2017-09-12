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

import org.junit.Ignore;
import org.junit.Test;

public class DynectClientTest {
	
	DynectClient client;
	
	@Test
	@Ignore
	public void test() {
		
		client = new DynectClient.DynBuilder().withProperties("Example", "mercator", "test").build();
		client.get("Zone");

	}

}
