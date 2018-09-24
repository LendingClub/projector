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
/**
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
package org.lendingclub.mercator.jenkins;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

import io.macgyver.okrest3.OkRestTarget;

public interface JenkinsClient {


	/**
	 * Convenience method gets a list of job names.
	 * @return list of job names
	 */
	List<String> getJobNames();
	
	JsonNode getJson(String path);

	JsonNode getServerInfo();
	
	String getServerUrl();
	
	JsonNode getJob(String jobName);

	JsonNode getBuild(String jobName, int buildNumber);

	org.jdom2.Document getJobConfig(String jobName);
	
	String executeGroovyScript(String groovy);

	JsonNode build(String name);
	
	JsonNode buildWithParameters(String name, String ... args);
	
	JsonNode buildWithParameters(String name, Map<String,String> params);
	
	JsonNode getBuildQueue();
	JsonNode getLoadStats();
	
	void restart();
	void restartAfterJobsComplete();
	
	void quietDown();
	void cancelQuietDown();
	
	OkRestTarget getOkRestTarget();
	
}
