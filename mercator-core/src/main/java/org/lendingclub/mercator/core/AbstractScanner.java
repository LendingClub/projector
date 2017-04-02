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

import java.util.Map;

import org.lendingclub.neorx.NeoRxClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;


public abstract class AbstractScanner implements Scanner {

	Logger logger = LoggerFactory.getLogger(getClass());

	ScannerBuilder<? extends Scanner> builder;
	Map<String, String> config;

	public AbstractScanner(ScannerBuilder<? extends Scanner> builder, Map<String, String> props) {
		this.builder = builder;
		config = Maps.newHashMap();
		Preconditions.checkNotNull(builder, "builder cannot be null");
		Preconditions.checkNotNull(builder.getProjector(), "builder.getProjector() cannot be null");
		config.putAll(builder.getProjector().getProperties());
		if (props != null) {
			config.putAll(props);
		}
	}

	public Map<String, String> getConfig() {
		return config;
	}

	@Override
	public Projector getProjector() {
		return builder.getProjector();
	}

	public NeoRxClient getNeoRxClient() {
		return getProjector().getNeoRxClient();
	}

	public boolean isFailOnError() {
		return builder.isFailOnError();
	}

	public void maybeThrow(Exception e, String message) {
		ScannerContext.getScannerContext().ifPresent(sc -> {
			sc.markException(e);
		});
		if (isFailOnError()) {
			if (e instanceof RuntimeException) {
				throw ((RuntimeException) e);
			} else {
				throw new MercatorException(e);
			}
		} else {
			logger.warn(message, e);
		}
	}

	public void maybeThrow(Exception e) {
		maybeThrow(e, "scanning problem");

	}

	public SchemaManager getSchemaManager() {
		return new SchemaManager(getProjector().getNeoRxClient());
	}

}
