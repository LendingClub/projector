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
package org.lendingclub.mercator.kubernetes;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.lendingclub.mercator.core.Projector;

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import io.reactivex.Single;
import io.reactivex.functions.Consumer;

public class KubeEventScannerTest extends KubeIntegrationTest {



	@Test
	public void testEvents() throws Exception {
		

		getTestKubeScanner().scan();
		Assertions.assertThat(getTestKubeScanner().getEventScanner()).isSameAs(getTestKubeScanner().getEventScanner());
		
		CountDownLatch cdl = new CountDownLatch(1);
		getTestKubeScanner().getEventScanner().withConsumer(c->{
			
		//	System.out.println(c.getResource().getClass());
		});
	
		Assertions.assertThat(getTestKubeScanner().getKubernetesClient()).isSameAs(getTestKubeScanner().getKubernetesClient());

		getTestKubeScanner().getEventScanner().watchAll();
		
		//Thread.sleep(60000);
		
	}
}
