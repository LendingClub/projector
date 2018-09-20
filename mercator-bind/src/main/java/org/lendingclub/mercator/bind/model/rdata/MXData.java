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

public final class MXData extends HashMap<String, Object> {

	private static final long serialVersionUID = -3733294665597757116L;
	
	public MXData(int preference, String exchange) {
		Preconditions.checkArgument(preference <= 0xFFFF, "preference must be 65535 or less");
		Preconditions.checkNotNull(exchange, "exchange can't be null");
	    put("preference", preference);
	    put("exchange", exchange);
	}
	
	public static MXData create (int preference, String exchange) {
		return new MXData(preference, exchange);
	}
	
	public int preference() {
		return Integer.class.cast(get("preference"));
	}
	
	public String exchange() {
		return get("exchange").toString();
	}

}
