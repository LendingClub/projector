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
package org.lendingclub.mercator.bind.model.rdata;

import java.util.HashMap;

import com.google.common.base.Preconditions;


public final class SOAData extends HashMap<String, Object> {

	private static final long serialVersionUID = 7203990813489971720L;
	
	public SOAData(String pName, String dName, int serial, int refresh, int retry, int expire, int minimum ) {
		Preconditions.checkNotNull(pName, "primary name server for the domain can't be empty");
		Preconditions.checkNotNull(dName, "responsible party for the domain can't be empty");
		Preconditions.checkArgument(serial >=0 , "serial of %s must be unsigned", pName);
		Preconditions.checkArgument(refresh >=0 , "refresh of %s must be unsigned", pName);
		Preconditions.checkArgument(retry >=0 , "retry of %s must be unsigned", pName);
		Preconditions.checkArgument(expire >=0 , "expire of %s must be unsigned", pName);
		Preconditions.checkArgument(minimum >=0 , "minimum of %s must be unsigned", pName);
		
		put("primaryDomainNameServer", pName);
		put("domainServerName", dName);
		put("serial", serial);
		put("refresh", refresh);
		put("retry", retry);
		put("expire", expire);
		put("minimum", minimum);
	}
	
	public static SOAData.Builder builder() {
		return new Builder();
	}
	
	public String primaryDomainNameServerName() {
		return get("primaryDomainNameServer").toString();
	}
	
	public String domainServerName() {
		return get("domainServerName").toString();
	}
	
	public int serial() {
		return Integer.class.cast(get("serial"));
	}
	
	public int refresh() {
		return Integer.class.cast(get("refresh"));
	}
	
	public int retry() {
		return Integer.class.cast(get("retry"));
	}
	
	public int expire() {
		return Integer.class.cast(get("expire"));
	}
	
	public int minimum() {
		return Integer.class.cast(get("minimum"));
	}
	
	public final static class Builder {
		
		private String pName;
		private String dName;
		private int serial = -1;
		private int refresh = -1;
		private int retry = -1;
		private int expire = -1;
		private int minimum = -1;
		
		public SOAData.Builder primaryDomainNameServer(String pName) {
			this.pName = pName;
			return this;
		}
		
		public SOAData.Builder domainServerName(String dName) {
			this.dName = dName;
			return this;
		}
		
		public SOAData.Builder serial(int serial) {
			this.serial = serial;
			return this;
		}
		
		public SOAData.Builder refresh(int refresh) {
			this.refresh = refresh;
			return this;
		}
		
		public SOAData.Builder retry(int retry) {
			this.retry = retry;
			return this;
		}
		
		public SOAData.Builder expire(int expire) {
			this.expire = expire;
			return this;
		}
		
		public SOAData.Builder minimum(int minimum) {
			this.minimum = minimum;
			return this;
		}
		
		public SOAData build() {
			return new SOAData(pName, dName, serial, refresh, retry, expire, minimum);
		}		
	}

}
