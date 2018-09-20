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

import org.lendingclub.neorx.NeoRxClient;

import com.google.common.base.Strings;

public abstract class Projector {


	public abstract NeoRxClient getNeoRxClient();

	public abstract <T extends ScannerBuilder> T createBuilder(Class<T> clazz);

	public static class Builder {
		NeoRxClient neorx;
		String url;
		String username;
		String password;
		java.util.function.Consumer<NeoRxClient.Builder> config;
	
		public Builder withNeoRxClient(NeoRxClient c) {
			this.neorx = c;
			return this;
		}

		public Builder withUrl(String url) {
			this.url = url;
			return this;
		}

		public Builder withNeoRxConfig(java.util.function.Consumer<NeoRxClient.Builder> cfg) {
			this.config = cfg;
			return this;
		}
		public Builder withUsername(String username) {
			this.username = username;
			return this;
		}

		public Builder withPassword(String password) {
			this.password = password;
			return this;
		}
	

		public Projector build() {
			if (neorx != null) {
				return new BasicProjector(neorx);
			}
			
			
			NeoRxClient.Builder neorxBuilder = new NeoRxClient.Builder();
			

			if (!Strings.isNullOrEmpty(url)) {
				neorxBuilder = neorxBuilder.withUrl(url);
				
			}
			if ((!Strings.isNullOrEmpty(username)) && (!Strings.isNullOrEmpty(password))) {
				neorxBuilder = neorxBuilder.withCredentials(username, password);
			}
			
			if (config!=null) {
				
				config.accept(neorxBuilder);
				
			}
			return new BasicProjector(neorxBuilder.build());
		}
	}
}
