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
package org.lendingclub.mercator.solarwinds;

import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;
import org.lendingclub.mercator.core.ScannerBuilder;

public class SolarwindsScannerBuilder extends ScannerBuilder<SolarwindsScanner> {

    String username;
    String password;
    String url;
    String hashURL;
    boolean validateCertificates=true;

    @Override
    public SolarwindsScanner build(){return new SolarwindsScanner(this);}

    public SolarwindsScannerBuilder withUrl(String url) {
        this.url = url;
        this.hashURL = Hashing.sha1().hashString(url, Charsets.UTF_8).toString();
        return this;
    }
    public SolarwindsScannerBuilder withUsername(String username) {
        this.username = username;
        return this;
    }
    public SolarwindsScannerBuilder withPassword(String password) {
        this.password = password;
        return this;
    }
    public SolarwindsScannerBuilder withCertValidationEnabled(boolean b) {
        this.validateCertificates = b;
        return this;
    }
}
