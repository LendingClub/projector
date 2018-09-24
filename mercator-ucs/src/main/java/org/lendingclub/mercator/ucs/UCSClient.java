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

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;
import org.lendingclub.mercator.core.MercatorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;

import io.macgyver.okrest3.OkHttpClientConfigurer;
import io.macgyver.okrest3.OkRestClient;
import io.macgyver.okrest3.OkRestClientConfigurer;
import io.macgyver.okrest3.OkRestResponse;
import io.macgyver.okrest3.OkRestTarget;

public class UCSClient {

	static Logger logger = LoggerFactory.getLogger(UCSClient.class);

	OkRestTarget target;
	Builder builder;
	String username;
	String password;

	TokenSupplier tokenSupplier = new TokenSupplier();

	public class Token {
		String value;
		int refreshPeriod = 300;
		long refreshTime = System.currentTimeMillis();

		public String getTokenValue() {
			return value;
		}

		public int getRefreshPeriod() {
			return refreshPeriod;
		}

		public boolean isOlderThan(long t, TimeUnit unit) {
			return System.currentTimeMillis() - refreshTime > unit.toMillis(t);
		}
	}

	AtomicReference<Token> tokenRef = new AtomicReference<>();

	public static class Builder {
		String url;
		String username;
		String password;
		OkRestClientConfigurer okRestConfigurer;
		OkHttpClientConfigurer okHttpConfigurator;
		boolean validateCertificates = true;

		public Builder withUrl(String url) {
			this.url = url;
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

		public Builder withCertValidationEnabled(boolean b) {
			this.validateCertificates = b;
			return this;
		}

		public Builder withConfig(OkRestClientConfigurer okRestConfig) {
			this.okRestConfigurer = okRestConfig;
			return this;
		}

		public UCSClient build() {
			UCSClient client = new UCSClient();
			OkRestClient.Builder builder = new OkRestClient.Builder();
			if (!validateCertificates) {
				builder.disableCertificateVerification();
			}
			if (okRestConfigurer != null) {
				builder.withOkRestClientConfig(okRestConfigurer);
			}
			if (okHttpConfigurator != null) {
				builder.withOkHttpClientConfig(okHttpConfigurator);
			}
			logger.info("constructing UCSClient for {}",url);
			client.target = builder.build().url(url);
			client.username = username;
			client.password = password;
			return client;
		}
	}

	class TokenSupplier implements Supplier<Token> {

		public Token get() {
			try {
				logger.info("authenticating to {}",target.getUrl());
				Element el = new Element("aaaLogin");

				el.setAttribute("inName", username);
				el.setAttribute("inPassword", password);

				XMLOutputter out = new XMLOutputter();
				out.setFormat(Format.getPrettyFormat());

				OkRestResponse response = target.post(out.outputString(el)).execute();

				String contentType = response.response().header("Content-type");
				if (!Strings.isNullOrEmpty(contentType)) {
					if (contentType.contains("html")) {
						throw new UCSException("unexpected content-type: " + contentType);
					}
				}

				String val = response.getBody(String.class);
				SAXBuilder saxBuilder = new SAXBuilder();
				
				Document document = saxBuilder.build(new StringReader(val));
				logDebug("auth result", document.getRootElement());

				//temporarily logging this to check what's going wrong in prod
				logInfo("auth result", document.getRootElement());

				Token token = new Token();
				token.value = document.getRootElement().getAttributeValue("outCookie");
				token.refreshPeriod = 300;
				try {
					token.refreshPeriod = Integer.parseInt(document.getRootElement().getAttributeValue("outRefreshPeriod"));
				}
				catch(Exception e) {

				}
				if (Strings.isNullOrEmpty(token.value)) {
					throw new UCSException("Authentication failed");
				}
				logger.info("successfully authenticated to {}",target.getUrl());
				return token;
			} catch (IOException | JDOMException e) {
				throw new MercatorException(e);
			}
		}

	}

	private Token refresh(Token token) {
		return token;
	}

	public void logout() {
		Token token = tokenRef.get();
		if (token != null) {
			tokenRef.set(null);
			String tokenValue = token.getTokenValue();
			if (!Strings.isNullOrEmpty(tokenValue)) {
				logger.info("closing session to {}...",target.getUrl());
				Element result = exec("aaaLogout", "inCookie", token.getTokenValue());
				logDebug("logout", result);
				logger.info("successfully closed session to {}",target.getUrl());
				
			}
		}
	}

	protected void forceLogout() {
		tokenRef.set(null);
	}

	public Token getToken() {
		Token token = tokenRef.get();
		if (token == null) {
			token = tokenSupplier.get();
		}
		if (token.isOlderThan(5, TimeUnit.MINUTES)) {
			token = refresh(token);
		} else {
			logger.debug("token does not need to be refreshed");
		}
		tokenRef.set(token);
		return token;
	}

	public void logDebug(String message, Element element) {

		if (logger.isDebugEnabled()) {
			XMLOutputter out = new XMLOutputter();

			out.setFormat(Format.getPrettyFormat());

			element = element.clone();
			if (!Strings.isNullOrEmpty(element.getAttributeValue("outCookie"))) {
				element.setAttribute("outCookie", "**********");
			}
			if (!Strings.isNullOrEmpty(element.getAttributeValue("cookie"))) {
				element.setAttribute("cookie", "**********");
			}
			logger.debug(message + "\n{}", out.outputString(element));
		}
	}

	public void logInfo(String message, Element element) {

		XMLOutputter out = new XMLOutputter();

		out.setFormat(Format.getPrettyFormat());

		element = element.clone();
		if (!Strings.isNullOrEmpty(element.getAttributeValue("outCookie"))) {
			element.setAttribute("outCookie", "**********");
		}
		if (!Strings.isNullOrEmpty(element.getAttributeValue("cookie"))) {
			element.setAttribute("cookie", "**********");
		}
		logger.info(message + "\n{}", out.outputString(element));

	}

	public List<Element> resolveClass(String classId, boolean inHierarchical) {
		Element r = exec("configResolveClass", "classId", classId, "inHierarchical", Boolean.toString(inHierarchical));

		return r.getChild("outConfigs").getChildren();
	}

	public List<Element> resolveClass(String classId) {
		return resolveClass(classId, false);
	}

	public Element resolveDn(String dn) {

		Preconditions.checkArgument(!Strings.isNullOrEmpty(dn), "dn not set");
		Element r = exec("configResolveDn", "dn", dn);

		return r;

	}

	public Map<String, String> toMap(String... args) {
		Map<String, String> map = Maps.newHashMap();
		if (args == null) {
			return map;
		}
		if (args.length % 2 != 0) {
			throw new IllegalArgumentException("arguments must be provided in even numbers (pairs)");
		}
		for (int i = 0; i * 2 + 1 < args.length; i++) {
			map.put(args[i * 2], args[i * 2 + 1]);
		}
		return map;
	}

	public Element exec(String command, String... args) {
		return exec(command, toMap(args));
	}

	public Element exec(String command, Map<String, String> args) {
		try {
			Element el = new Element(command);

			if (!command.equals("aaaLogout")) {
				String cookie = getToken().getTokenValue();
				if (Strings.isNullOrEmpty(cookie)) {
					throw new MercatorException("client does not have valid session");
				}
				el.setAttribute("cookie", getToken().getTokenValue());
			}

			args.forEach((k, v) -> {
				el.setAttribute(k, v);
			});
			XMLOutputter out = new XMLOutputter();
			String val = target.post(out.outputString(el)).header("Content-type", "application/xml")
					.execute(String.class);

			SAXBuilder saxBuilder = new SAXBuilder();

			Document document = saxBuilder.build(new StringReader(val));

			Element element = document.getRootElement();
			logDebug(command, element);
			String errorCode = element.getAttributeValue("errorCode");
			if (!Strings.isNullOrEmpty(errorCode)) {
				if (errorCode.equals("552")) {
					logout();
					throw UCSRemoteException.fromResponse(element);
				} else if (errorCode.equals("555") && command.equals("aaaLogout")) {
					logger.info("aaaLogout session not found");
				} else {
					throw UCSRemoteException.fromResponse(element);
				}
			}
			return element;
		} catch (IOException | JDOMException e) {
			throw new UCSException(e);
		}
	}

	public String getUCSManagerId() {

		return Hashing.sha1().hashString(target.getUrl().toLowerCase(), Charsets.UTF_8).toString();
	}

	public List<Element> resolveChildren(String dn, String classId) {

		Map<String, String> m = Maps.newHashMap();
		m.put("inDn", dn);
		if (classId != null) {
			m.put("classId", classId);
		}
		Element result = exec("configResolveChildren", m);

		return result.getChild("outConfigs").getChildren();
	}

}
