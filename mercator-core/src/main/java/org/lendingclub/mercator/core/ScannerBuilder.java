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

import java.util.Optional;

public abstract class ScannerBuilder<T> {

	Projector projector;
	boolean failOnError = false;
	Double rateLimitPerSec=null;
	public void setProjector(Projector p) {
		this.projector = p;
	}
	public Projector getProjector() {
		return projector;
	}
	public <X extends ScannerBuilder<T>> X withFailOnError(boolean b) {
		this.failOnError = b;
		return (X) this;
	}	
	public boolean isFailOnError() {
		return failOnError;
	}
	
	public Optional<Double> getRateLimitPerSecond() {
		return java.util.Optional.ofNullable(rateLimitPerSec);
	}
	public <X extends ScannerBuilder<T>> X withRateLimitPerSecond(double c) {
		this.rateLimitPerSec = c;
		return (X) this;
	}
	public abstract T build();
}
