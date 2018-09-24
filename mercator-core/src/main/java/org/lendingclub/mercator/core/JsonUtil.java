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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JsonUtil {

	static final ObjectMapper mapper = new ObjectMapper();
	static Logger logger = LoggerFactory.getLogger(JsonUtil.class);

	public static ObjectMapper getObjectMapper() {
		return mapper;
	}

	public static ObjectNode createObjectNode() {
		return getObjectMapper().createObjectNode();
	}

	public static ArrayNode createArrayNode() {
		return getObjectMapper().createArrayNode();
	}

	public static String prettyFormat(Object n) {
		try {
			return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(n);
		} catch (JsonProcessingException e) {
			throw new MercatorException(e);
		}
	}

	private static Class getCallerClass() {
		Class clazz = null;
		try {

			int position = 3;
			clazz = Class.forName(Thread.currentThread().getStackTrace()[position].getClassName());
		} catch (Exception e) {
			clazz = JsonUtil.class;
		}
		return clazz;
	}

	public static void logInfo(String message, Object n) {

		
		logInfo(getCallerClass(), message, n);
	}

	public static void logInfo(Logger logger, String message, Object n) {
		try {
			if (logger != null && logger.isInfoEnabled()) {

				logger.info("{} - \n{}", message, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(n));
			}
		} catch (JsonProcessingException e) {
			logger.warn("problem logging: {}", e.toString());
		}
	}

	public static void logInfo(Class z, String message, Object n) {

		if (z != null) {
			logInfo(LoggerFactory.getLogger(z), message, n);
		}

	}

	public static void logDebug(String message, Object n) {
		logDebug(getCallerClass(),message,n);
	}
	
	public static void logDebug(Class z, String message, Object n) {

		if (z != null) {
			logDebug(LoggerFactory.getLogger(z), message, n);
		}
	}

	public static void logDebug(Logger log, String message, Object n) {
		try {

			if (log != null && log.isDebugEnabled()) {
				log.debug("{} - \n{}", message, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(n));
			}
		} catch (

		JsonProcessingException e) {
			logger.warn("problem logging: {}", e.toString());
		}

	}
}
