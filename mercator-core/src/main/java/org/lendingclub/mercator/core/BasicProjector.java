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
package org.lendingclub.mercator.core;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.lendingclub.neorx.NeoRxClient;
import org.lendingclub.neorx.NeoRxClient.Builder;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

public class BasicProjector implements Projector {

	private NeoRxClient neorx;
	Map<String, String> properties;

	public BasicProjector() {
		this(Maps.newHashMap());
	}

	public BasicProjector(Map<String, String> properties) {
		collectConfig();
		this.properties.putAll(properties);
	}
	
	public BasicProjector(NeoRxClient client) {
		this();
		this.neorx = client;
	}

	private void collectConfig() {
		properties = Maps.newHashMap();
		properties.putAll(Maps.fromProperties(System.getProperties()));

		File projectorPropertiesFile = new File("./mercator.properties");
		if (projectorPropertiesFile.exists()) {
			try (FileReader fr = new FileReader(projectorPropertiesFile)) {
				Properties p = new Properties();
				p.load(fr);
				properties.putAll(properties);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	public Map<String, String> getProperties() {
		return properties;
	}

	public BasicProjector withNeoRxClient(NeoRxClient client) {
		this.neorx = client;
		return this;
	}
	@Override
	public NeoRxClient getNeoRxClient() {
		if (this.neorx == null) {
			String neo4jUrl = properties.getOrDefault("neo4j.url", "bolt://localhost:7687");
			String neo4jUsername = properties.get("neo4j.username");
			String neo4jPassword = properties.get("neo4j.password");

			Builder builder = new NeoRxClient.Builder();

			if (!Strings.isNullOrEmpty(neo4jUsername)) {
				builder = builder.withCredentials(neo4jUsername,neo4jPassword);
			}

			this.neorx = builder.build();
		}
		return neorx;
	}

	@Override
	public <T extends ScannerBuilder> T createBuilder(Class<T> clazz) {
		try {
			T t = (T) clazz.newInstance();
			t.setProjector(this);
			Preconditions.checkNotNull(t.getProjector());
			return t;
		} catch (IllegalAccessException | InstantiationException e) {
			throw new RuntimeException(e);
		}
	}

}
