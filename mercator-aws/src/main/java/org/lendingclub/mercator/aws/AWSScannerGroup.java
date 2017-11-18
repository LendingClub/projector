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
package org.lendingclub.mercator.aws;

import java.util.ArrayList;
import java.util.List;

import org.lendingclub.mercator.core.Scanner;

import com.amazonaws.AmazonWebServiceClient;
import com.google.common.collect.Lists;

//@SuppressWarnings("rawtypes")
public class AWSScannerGroup extends AWSScanner<AmazonWebServiceClient> {

	List<Class<? extends AWSScanner<?>>> scannerList = Lists.newCopyOnWriteArrayList();

	public AWSScannerGroup(AWSScannerBuilder builder) {
		super(builder, null, null);
	}

	public List<Class<? extends AWSScanner<?>>> getScannerTypes() {
		return scannerList;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public AWSScannerGroup addScannerType(Class<? extends AWSScanner> type) {
		scannerList.add((Class<? extends AWSScanner<?>>) type);
		return this;
	}

	@SuppressWarnings("rawtypes")
	public AWSScannerGroup removeScannerType(Class<? extends AWSScanner> type) {
		scannerList.remove(type);
		return this;
	}

	@Override
	protected void doScan() {
		for (Scanner scanner : getScanners()) {
			try {
				scanner.scan();
			} catch (RuntimeException e) {
				maybeThrow(e);
			}
		}

	}

	protected List<Scanner> getScanners() {
		List<Scanner> result = new ArrayList<>();
		for (Class<? extends AWSScanner<?>> scannerClass : scannerList) {
			AWSScanner<?> scanner = builder.build(scannerClass);
			if (scanner != null) {
				result.add(scanner);
			}
		}
		return result;
	}
}
