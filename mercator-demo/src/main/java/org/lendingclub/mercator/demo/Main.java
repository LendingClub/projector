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
package org.lendingclub.mercator.demo;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.lendingclub.mercator.aws.AWSScannerBuilder;
import org.lendingclub.mercator.aws.AllEntityScanner;
import org.lendingclub.mercator.core.BasicProjector;
import org.lendingclub.mercator.core.Projector;
import org.lendingclub.mercator.docker.DockerScannerBuilder;
import org.slf4j.bridge.SLF4JBridgeHandler;

import com.amazonaws.regions.Regions;

public class Main {

	public static void main(String [] args) {
		SLF4JBridgeHandler.removeHandlersForRootLogger();
		SLF4JBridgeHandler.install();
		Projector projector = new Projector.Builder().build();
		
		Runnable awsTask = new Runnable() {
			
			public void run() {
				projector.createBuilder(AWSScannerBuilder.class).withRegion(Regions.US_EAST_1).build(AllEntityScanner.class).scan();
				projector.createBuilder(AWSScannerBuilder.class).withRegion(Regions.US_WEST_2).build(AllEntityScanner.class).scan();
			}
		
		};
		
		Runnable dockerTask = new Runnable() {
			public void run() {
		
		projector.createBuilder(DockerScannerBuilder.class).withLocalDockerDaemon().build().scan();
			}
		};
		
		ScheduledExecutorService exec = Executors.newScheduledThreadPool(2);
		exec.scheduleWithFixedDelay(awsTask, 0, 1, TimeUnit.MINUTES);
		exec.scheduleWithFixedDelay(dockerTask, 0, 10, TimeUnit.SECONDS);
		

        while (true==true)  {
            try {
                Thread.sleep(1000);
            }
            catch (Exception e) {}
        }
	}

}
