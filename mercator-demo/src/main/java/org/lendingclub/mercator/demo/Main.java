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

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.lendingclub.mercator.aws.AWSScannerBuilder;
import org.lendingclub.mercator.aws.AllEntityScanner;
import org.lendingclub.mercator.core.BasicProjector;
import org.lendingclub.mercator.core.Projector;
import org.lendingclub.mercator.docker.DockerScannerBuilder;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import com.amazonaws.regions.Regions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

public class Main {

	public static void main(String[] args) {
		SLF4JBridgeHandler.removeHandlersForRootLogger();
		SLF4JBridgeHandler.install();
		Projector projector = new Projector.Builder().build();

		Runnable awsTask = new Runnable() {

			public void run() {
				try {
				List<String> regionList = Splitter.on(",").omitEmptyStrings().trimResults()
						.splitToList(Strings.nullToEmpty(System.getenv("AWS_REGIONS")));
			
				if (regionList==null || regionList.isEmpty()) {
					regionList = Lists.newArrayList();
					regionList.add("us-west-2");
					regionList.add("us-east-1");
				}
				LoggerFactory.getLogger(Main.class).info("scanning regions: {}",regionList);
				regionList.forEach(it -> {
					try {
						org.slf4j.LoggerFactory.getLogger(Main.class).info("scanning region: {}",it);
						Regions region = Regions.fromName(it);
						projector.createBuilder(AWSScannerBuilder.class).withRegion(region)
								.build(AllEntityScanner.class).scan();
					} catch (Exception e) {
						LoggerFactory.getLogger(Main.class).warn("problem scanning " + it, e);
					}
				});
				}
				catch (Exception e) {
					LoggerFactory.getLogger(Main.class).warn("problem",e);
				}
			}

		};

		Runnable dockerTask = new Runnable() {
			public void run() {

				projector.createBuilder(DockerScannerBuilder.class).withLocalDockerDaemon().build().scan();
			}
		};

		ScheduledExecutorService exec = Executors.newScheduledThreadPool(5);
		exec.scheduleWithFixedDelay(awsTask, 0, 1, TimeUnit.MINUTES);
		exec.scheduleWithFixedDelay(dockerTask, 0, 10, TimeUnit.SECONDS);

		while (true == true) {
			try {
				Thread.sleep(1000);
			} catch (Exception e) {
			}
		}
	}

}
