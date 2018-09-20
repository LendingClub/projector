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

import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import org.lendingclub.mercator.core.JsonUtil;

import java.util.List;
import java.util.function.Consumer;

public class SpotFleetRequestScanner extends AbstractEC2Scanner {

    public SpotFleetRequestScanner(AWSScannerBuilder builder){
        super(builder, "AwsSpotFleetRequest");
        jsonConverter.flattenNestedObjects = true;
    }

    public void scanSpotFleetRequests(String... spotFleetRequestIds){
        if (spotFleetRequestIds == null || spotFleetRequestIds.length == 0) {
            doScan();
        }else{
            doScan(spotFleetRequestIds);
        }
    }

    @Override
    protected void doScan() {
        doScan(new String[0]);
    }
    protected void doScan(String... spotFleetRequestIds) {

        GraphNodeGarbageCollector gc = newGarbageCollector();
        if (spotFleetRequestIds==null || spotFleetRequestIds.length==0){
            gc.bindScannerContext();
        }

        forEachFleetRequest(config ->{
            try{
                ObjectNode configNode = addLaunchSpecificationTags(config);
                ObjectNode n = convertAwsObject(configNode, getRegion());

                String cypher = "merge (a:AwsSpotFleetRequest {aws_spotFleetRequestId:{requestId}, aws_region:{region}}) set a+={props}, a.updateTs=timestamp() return a";

                Preconditions.checkNotNull(getNeoRxClient());
                getNeoRxClient().execCypher(cypher, "requestId", config.getSpotFleetRequestId(), "region", getRegion().getName(), "props", n).forEach(r -> {
                   gc.MERGE_ACTION.accept(r);
                   getShadowAttributeRemover().removeTagAttributes("AwsSpotFleetRequest", n, r);
                });
                incrementEntityCount();
            } catch (RuntimeException e){
                maybeThrow(e, "problem scanning spot fleet request");
            }

         }, spotFleetRequestIds);
    }

    private ObjectNode addLaunchSpecificationTags(SpotFleetRequestConfig config){
        ObjectMapper mapper = JsonUtil.getObjectMapper();
        //All launch specs are tagged the same, so just grab the first
        List<Tag> tags = config.getSpotFleetRequestConfig().getLaunchSpecifications().get(0).getTagSpecifications().get(0).getTags();
        ObjectNode n = mapper.valueToTree(config);
        n.set("tags", mapper.valueToTree(tags));
        return n;
    }

    private void forEachFleetRequest(Consumer<SpotFleetRequestConfig> consumer, String... spotFleetRequestIds) {

        DescribeSpotFleetRequestsRequest describeRequest = new DescribeSpotFleetRequestsRequest();
        if (spotFleetRequestIds != null && spotFleetRequestIds.length > 0) {
            describeRequest.withSpotFleetRequestIds(spotFleetRequestIds);
        }

        String token = null;
        do {
            rateLimit();
            DescribeSpotFleetRequestsResult results = getClient().describeSpotFleetRequests(describeRequest);
            token = results.getNextToken();
            results.getSpotFleetRequestConfigs().forEach(consumer);
            describeRequest.setNextToken(token);

        } while (tokenHasNext(token));
    }
}