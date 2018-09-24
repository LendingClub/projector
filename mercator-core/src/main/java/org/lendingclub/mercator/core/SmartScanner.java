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

import java.util.concurrent.TimeUnit;


public abstract class SmartScanner extends AbstractScanner {

	boolean smartScanEnabled=false;
	private static final long DEFAULT_ENTITY_SCAN_INTERVAL=TimeUnit.MINUTES.toMillis(5);
	private static final long DEFAULT_FULL_SCAN_INTERVAL=TimeUnit.MINUTES.toMillis(30);
	private long entityScanInterval=DEFAULT_ENTITY_SCAN_INTERVAL;
	private long fullScanInterval=DEFAULT_FULL_SCAN_INTERVAL;
	public SmartScanner(ScannerBuilder<? extends Scanner> builder) {
		super(builder);
	
	}
	public abstract ScanControl getScanControl();
	
	public long getEntityScanIntervalMillis() {
		return entityScanInterval;
	}
	public long getFullScanIntervalMillis() {
		return fullScanInterval;
	}
	protected <X extends SmartScanner> X withEntityScanInterval(long duration, TimeUnit unit) {
		this.entityScanInterval = unit.toMillis(duration);
		return (X) this;
	}

	protected <X extends SmartScanner> X withFullScanInterval(long duration, TimeUnit unit) {
		fullScanInterval = unit.toMillis(duration);
		return (X) this;
	}
	
	public boolean isSmartScanEnabled() {
		return smartScanEnabled;
	}
	public <T extends SmartScanner> T withSmartScanEnabled(boolean b) {
		this.smartScanEnabled = b;
		return (T) this;
	}

	
	public boolean isFullScanRequired() {
		return isSmartScanEnabled()==false;
	}
	
	public abstract void markFullScan();
	
	public final void fullScan() {
		doFullScan();
		markFullScan();
	}
	/**
	 * If we call for a smart scan, we may still be reverted to a full scan.
	 */
	public final void smartScan() {

		if (isFullScanRequired()) {
			fullScan();
		}
		else {
			doSmartScan();
		}		
	}

	public final void scan() {
		if (!isSmartScanEnabled()) {
			logger.warn("{} does not have smart scan support...falling back to full scan",getClass().getName());
			fullScan();
			return;
		}
		else {
			logger.info("performing smart scan");
			smartScan();
		}
	}

	protected abstract void doSmartScan();
	protected abstract void doFullScan();
}
