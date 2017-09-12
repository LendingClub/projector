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
package org.lendingclub.mercator.solarwinds;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lendingclub.mercator.core.Projector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SolarwindsScannerIntegrationTest {

    static Logger logger = LoggerFactory.getLogger(SolarwindsScannerIntegrationTest.class);
    static SolarwindsScanner solarwindsScanner = null;
    public SolarwindsScanner getSolarwindsScanner(){return solarwindsScanner;}

    @BeforeClass
    public static void setup(){
        SolarwindsScanner ss = new Projector.Builder().build().createBuilder(SolarwindsScannerBuilder.class).build();
        boolean b = ss.getNeoRxClient().checkConnection();
        if (b==false) {
            logger.warn("neo4j is not available...integration tests will be skipped");
            Assume.assumeTrue("neo4j available", b);
        }
        else {
            logger.info("neo4j is available for integration tests");
        }

    }
    @Test
    public void testIt(){

        SolarwindsScanner ss = new Projector.Builder().build().createBuilder(SolarwindsScannerBuilder.class)
                .withCertValidationEnabled(false)
                .withUrl("https://solarwinds.tlcinternal.com:17778/SolarWinds/InformationService/v3/Json/Query")
                .withUsername("")
                .withPassword("")
                .build();
        ss.scan();
    }
}
