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

package org.lendingclub.mercator.newrelic;

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.macgyver.okrest3.OkRestTarget;

public interface NewRelicClient {
	public static final String ENDPOINT_URL = "https://api.newrelic.com/";
	public static final String MEDIA_TYPE_APPLICATION_JSON = "application/json";
	public static final String APPLICATION_FORM_URLENCODED_VALUE = "application/x-www-form-urlencoded";
	
	public OkRestTarget getOkRestTarget();
	public ObjectNode get(String path, String... args);
	public ObjectNode getApplications();
	public ObjectNode getServers();
	public ObjectNode getAlertPolicies();
	public ObjectNode getAlertConditionForPolicy(String policyId);
	public String getAccountId();
}
