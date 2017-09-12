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

import java.util.HashMap;
import java.util.Map;

import org.lendingclub.mercator.bind.model.rdata.AData;
import org.lendingclub.mercator.bind.model.rdata.CNAMEData;
import org.lendingclub.mercator.bind.model.rdata.MXData;
import org.lendingclub.mercator.bind.model.rdata.NSData;
import org.lendingclub.mercator.bind.model.rdata.SOAData;
import org.lendingclub.mercator.bind.model.rdata.SRVData;
import org.lendingclub.mercator.bind.model.rdata.TXTData;

public enum ToDnsRecord {
	INSTANCE;
	
	public static Map<String, Object> toRData(String type, String[] data) {
		
		if ("A".equals(type)) {
		    return AData.create(data[4]);
		} else if("CNAME".equals(type)) {
			return CNAMEData.create(data[4]);	
		} else if ("NS".equals(type)) { 
			return NSData.create(data[4]);
		} else if ("MX".equals(type)) { 
			return MXData.create(Integer.parseInt(data[4]), data[5]);
		} else if ("TXT".equals(type)) {
			return TXTData.create(data[4]);
		} else if ("SOA".equals(type)) {
			return SOAData.builder().primaryDomainNameServer(data[4])
					.domainServerName(data[5])
					.serial(Integer.parseInt(data[6]))
					.refresh(Integer.parseInt(data[7]))
					.retry(Integer.parseInt(data[8]))
					.expire(Integer.parseInt(data[9]))
					.minimum(Integer.parseInt(data[10]))
					.build();
		} else if("SRV".equals(type)){
			return SRVData.builder().priority(Integer.parseInt(data[4]))
					.weight(Integer.parseInt(data[5]))
					.port(Integer.parseInt(data[6]))
					.target(data[7])
					.build();
		}else {
			Map<String, Object> builder = new HashMap<String, Object>();		
			return builder;
		}
	}

}
