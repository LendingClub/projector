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

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import io.macgyver.okrest3.OkRestClient;
import org.lendingclub.mercator.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class SolarwindsScanner extends AbstractScanner {

    Logger logger = LoggerFactory.getLogger(SolarwindsScanner.class);

    SolarwindsScannerBuilder solarwindsScannerBuilder;
    ObjectMapper mapper = new ObjectMapper();
    OkRestClient okRestClient;

    public SolarwindsScanner(ScannerBuilder<? extends Scanner> builder) {
        super(builder);
        this.solarwindsScannerBuilder = (SolarwindsScannerBuilder) builder;

        okRestClient = new OkRestClient.Builder()
                .withBasicAuth(solarwindsScannerBuilder.username, solarwindsScannerBuilder.password)
                .disableCertificateVerification().build();
    }

    public void scan(){
        logger.info("scanning Nodes...");
        getNodeInformation();
    }

    public ObjectNode querySolarwinds(String query) throws IOException, NoSuchAlgorithmException, KeyManagementException {
        ObjectNode body = mapper.createObjectNode().put("query", query);
        ObjectNode response = okRestClient.uri(solarwindsScannerBuilder.url)
                .contentType("application/json")
                .post(body)
                .execute(ObjectNode.class);
        return response;
    }

    ObjectNode flattenNode(JsonNode v ){
        Iterator keys = v.fieldNames();
        ObjectNode n = mapper.createObjectNode();
        while(keys.hasNext()){
            //Set first letter of property name to lowercase
            String key = keys.next().toString();
            String lowercaseKey = key.substring(0,1).toLowerCase()+key.substring(1);
            n.set(lowercaseKey, v.path(key));
        }
        return n;
    }

    public void getNodeInformation(){
        try {
            ObjectNode response = querySolarwinds("SELECT Nodes.NodeID, Nodes.SysName, Nodes.Caption, " +
                    "Nodes.Description, Nodes.IOSVersion, Nodes.CustomProperties.SerialNumber, Nodes.MachineType, " +
                    "Nodes.Vendor, Nodes.IPAddress, Nodes.SysObjectID, Nodes.DNS, Nodes.ObjectSubType, " +
                    "Nodes.Status, Nodes.StatusDescription, Nodes.CustomProperties.Department, Nodes.Location," +
                    " Nodes.CustomProperties.City FROM Orion.Nodes ORDER BY Nodes.SysName");

            AtomicLong earlistUpdate = new AtomicLong(Long.MAX_VALUE);
            AtomicBoolean error = new AtomicBoolean(false);
            response.path("results").forEach( v ->{
                try {
                    //solarwindsID is the hashedURL+nodeID
                    getProjector().getNeoRxClient()
                            .execCypher("merge(a: SolarwindsNode {solarwindsID:{solarwindsID}}) set a+={props}, a.updateTs=timestamp() return a",
                                    "solarwindsID", solarwindsScannerBuilder.hashURL+v.path("NodeID"), "props", flattenNode(v))
                            .blockingFirst(MissingNode.getInstance());
                }catch (Exception e){
                    logger.warn("problem", e);
                    error.set(true);
                }

            });
            if (error.get() == false){
                getNeoRxClient().execCypher(
                        "match(a: SolarwindsNode) where a.solarwindsID={solarwindsID} and  a.updateTs<{cutoff} detach delete a",
                        "solarwindsID", solarwindsScannerBuilder.hashURL, "cutoff", earlistUpdate.get());
            }
        }catch (Exception e){
            logger.info(e.toString());
        }
    }
}
