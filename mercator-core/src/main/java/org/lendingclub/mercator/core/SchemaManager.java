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

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.lendingclub.neorx.NeoRxClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Suppliers;
import com.google.common.collect.Sets;

public class SchemaManager {

	Logger logger = LoggerFactory.getLogger(getClass());
	
	NeoRxClient client;
	
	private static final boolean FAIL_ON_ERROR_DEFAULT=false;
	
	public SchemaManager(NeoRxClient client) {
		this.client = client;
	}
	public NeoRxClient getNeoRxClient() {
		return client;
	}
	public void applyConstraints() {
		
	}
	public void applyConstraint(String constraint) {
		applyConstraint(constraint,false);
	}
	
	public void applyUniqueConstraint(String label, String attribute) {
		applyUniqueConstraint(label,attribute,FAIL_ON_ERROR_DEFAULT);
	}
	public void applyUniqueConstraint(String label, String attribute,boolean failOnError) {
		if (uniqueConstraintExists(label, attribute)) {
			logger.info("unique constraint on {}.{} already exists",label,attribute);
			return;
		}
		applyConstraint(String.format("CREATE CONSTRAINT ON (x:%s) ASSERT x.%s IS UNIQUE ",label,attribute),failOnError);		
	}
	
	public void dropUniqueConstraint(String label, String attribute) {
		dropUniqueConstraint(label,attribute,FAIL_ON_ERROR_DEFAULT);
	}
	public void dropUniqueConstraint(String label,String attribute, boolean failOnError) {
		applyConstraint(String.format("DROP CONSTRAINT ON (x:%s) ASSERT x.%s IS UNIQUE ",label,attribute),failOnError);
	}
	public boolean uniqueConstraintExists(String label, String attribute) {
		String name = label+"."+attribute;
		return constraintSupplier.get().contains(name);
	}
	Supplier<Set<String>> constraintSupplier = Suppliers.memoizeWithExpiration(this::fetchUniqueConstraints,5,TimeUnit.SECONDS);
	private Set<String> fetchUniqueConstraints() {
		Set<String> uniqueConstraints = Sets.newHashSet();
		getNeoRxClient().execCypher("call db.constraints").forEach(it->{
			
			Pattern p = Pattern.compile("CONSTRAINT\\s*ON\\s*\\(((.+):(.+))\\)\\s+ASSERT((.*)\\.(.*))\\s+IS\\s+UNIQUE");
			String constraint = it.asText();
			
				Matcher m = p.matcher(constraint);
				
				if (m.matches()) {
					
				
					String label = m.group(3).trim();
					String attribute = m.group(6).trim();
					logger.info("unique constraint on {}.{} - {}",label,attribute,constraint);
					uniqueConstraints.add(label+"."+attribute);
				}
			
		});
		return uniqueConstraints;
	}
	public void applyConstraint(String c, boolean failOnError) {
		try {
			logger.info("altering constraint: {}",c);
			client.execCypher(c);
		} catch (RuntimeException e) {
			if (failOnError) {
				throw e;
			} else {
				logger.warn("problem altering constraint: " + c, e);
			}
		}
	}
}
