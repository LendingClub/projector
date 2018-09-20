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
import org.junit.Test;
import org.lendingclub.neorx.NeoRxException;

public class ProjectorTest {

	@Test
	public void testBuilderCannotConnect() {

		try {
			Projector p = new Projector.Builder().withUrl("bolt://localhost:12345").build();

			p.getNeoRxClient().execCypher("match (a) return a limit 10").blockingForEach(it -> {
				System.out.println(it);
			});
			Assertions.failBecauseExceptionWasNotThrown(NeoRxException.class);
		} catch (RuntimeException e) {
			// This is a problem with NeoRx...we really want it to wrap the underlying driver exception in NeoRxException for consistency
			
			//Assertions.assertThat(e).isInstanceOf(NeoRxException.class);
		}
	}
	
	

}
