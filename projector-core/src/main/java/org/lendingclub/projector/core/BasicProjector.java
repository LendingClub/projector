package org.lendingclub.projector.core;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import io.macgyver.neorx.rest.NeoRxClient;
import io.macgyver.neorx.rest.NeoRxClientBuilder;

public class BasicProjector implements Projector {

	private NeoRxClient neorx;
	Map<String,String> properties;
	
	public BasicProjector() {
		this(Maps.newHashMap());
	}
	public BasicProjector(Map<String,String> properties) {
		collectConfig();
		properties.putAll(properties);
		
		
	}
	private void collectConfig() {
		properties = Maps.newHashMap();
		properties.putAll(Maps.fromProperties(System.getProperties()));

		File projectorPropertiesFile = new File("./projector.properties");
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

	public Map<String,String> getProperties() {
		return properties;
	}
	@Override
	public NeoRxClient getNeoRx() {
		if (this.neorx==null) {
			String neo4jUrl = properties.getOrDefault("neo4j.url", "http://localhost:7474");
			String neo4jUsername = properties.get("neo4j.username");
			String neo4jPassword = properties.get("neo4j.password");

			NeoRxClientBuilder builder = new NeoRxClientBuilder().url(neo4jUrl);

			if (!Strings.isNullOrEmpty(neo4jUsername)) {
				builder = builder.credentials(neo4jUsername, neo4jPassword);
			}

			this.neorx = builder.build();	
		}
		return neorx;
	}


}
