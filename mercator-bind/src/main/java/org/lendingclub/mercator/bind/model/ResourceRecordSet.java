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
package org.lendingclub.mercator.bind.model;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;

public class ResourceRecordSet <T extends Map<String, Object>> {
	
	  private String name;
	  private String type;
	  private Integer ttl;
	  private String classType;
	  private T recordData;
	  private static ObjectMapper mapper = new ObjectMapper();

	  
	  public ResourceRecordSet(String name, Integer ttl, String type, String classType, T records) {
		    this.name = name;
		    this.type = Preconditions.checkNotNull(type, "type of %s", name);
		    
		    if (ttl != null) {
		        boolean rfc2181 = ttl >= 0 && ttl.longValue() <= 0x7FFFFFFFL;
		        Preconditions.checkArgument(rfc2181, "Invalid ttl value: %s, must be 0-2147483647", ttl);
		    }
		    this.ttl = ttl;
		    this.classType = classType;
		    this.recordData = records != null ? records : (T) Collections.EMPTY_MAP;
	  }
	  
	  public ResourceRecordSet<T> withName(String name) {
		  
		  Preconditions.checkNotNull(name, "dns name can't be null");
		  this.name = name;
          return this;
	  }
	  
	  public ResourceRecordSet<T> withTtl(Integer ttl) {
		  if (ttl != null) {
		        boolean isCorrect = ttl >= 0 && ttl.longValue() <= 0x7FFFFFFFL;
		        Preconditions.checkArgument(isCorrect, "Invalid ttl value: %s, must be 0-2147483647", ttl);
		  }
		  this.ttl = ttl;
		  return this;
	  }
	  
	  public ResourceRecordSet<T> withClassType(String classType) {
		  this.classType = classType;
		  return this;
	  }
	  
	  public ResourceRecordSet<T> withRecordData(T recordData) {
		  this.recordData = recordData;
		  return this;
	  }
	  
	  public String getName() {
		  return name;
	  }

	  public String getType() {
		  return type;
	  }

	  public Integer getTtl() {
		  return ttl;
	  }

	  public T getRecordData() {
		  return recordData;
	  }
	
	  public ObjectNode toJson() {		
		  ObjectNode recordNode = mapper.createObjectNode();
		  recordNode.put("name", name).put("ttl", ttl).put("class", classType).put("type", type);
		  
		  ObjectNode rData = mapper.createObjectNode();
		  Set<String> keys = recordData.keySet();
			
		  for(String key: keys) {
			rData.put(key, recordData.get(key).toString());
		  }
		  
		  recordNode.put("rData", rData);	
		  return recordNode;	
	 }	  
}
