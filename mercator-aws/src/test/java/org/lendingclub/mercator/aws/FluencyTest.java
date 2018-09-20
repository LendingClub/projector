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
package org.lendingclub.mercator.aws;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.lendingclub.mercator.aws.AWSScannerBuilder;
import org.lendingclub.mercator.aws.AccountScanner;
import org.lendingclub.mercator.core.BasicProjector;
import org.lendingclub.mercator.core.Projector;

public class FluencyTest {

	@Test
	public void test() {
		Projector p = new Projector.Builder().build();
		
		Assertions.assertThat(p.createBuilder(AWSScannerBuilder.class)).isNotNull();
		Assertions.assertThat(p.createBuilder(AWSScannerBuilder.class).getProjector()).isNotNull();
		AccountScanner scanner = p.createBuilder(AWSScannerBuilder.class).buildAccountScanner();
		
		Assertions.assertThat(scanner.getNeoRxClient()).isSameAs(p.getNeoRxClient());
		Assertions.assertThat(scanner.getProjector()).isSameAs(p);
		
	
	}

}
