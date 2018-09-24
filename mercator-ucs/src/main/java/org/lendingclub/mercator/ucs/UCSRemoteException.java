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
package org.lendingclub.mercator.ucs;

import org.jdom2.Element;

public class UCSRemoteException extends UCSException {

	String code;
	String description;
	
	public UCSRemoteException(String code, String description) {
		super("code="+code+" description="+description);
		this.code = code;
		this.description = description;
	}
	
	
	public static UCSRemoteException fromResponse(Element element) {
		String code = element.getAttributeValue("errorCode");
		String desc = element.getAttributeValue("errorDescr");
		UCSRemoteException e = new UCSRemoteException(code, desc);
		return e;
	}
}
