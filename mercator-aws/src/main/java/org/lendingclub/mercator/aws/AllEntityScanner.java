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

public class AllEntityScanner extends AWSParallelScannerGroup {

	public AllEntityScanner(AWSScannerBuilder builder) {
		super(builder);

		addScannerType(Route53Scanner.class);
		addScannerType(IAMScannerGroup.class);
		
		addScannerType(EC2ScannerGroup.class);
		addScannerType(ASGScannerGroup.class);
		addScannerType(ELBScanner.class);
		addScannerType(RDSInstanceScanner.class);
		addScannerType(S3BucketScanner.class);
		addScannerType(SQSScanner.class);
		addScannerType(SNSScanner.class);

		// addScannerType(KinesisScanner.class); // Kinesis seems to be heavily
		// rate-limited...leave it off for now
	}

}
