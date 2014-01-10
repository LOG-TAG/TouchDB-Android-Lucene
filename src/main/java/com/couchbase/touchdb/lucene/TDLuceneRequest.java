package com.couchbase.touchdb.lucene;

import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.node.ObjectNode;

public class TDLuceneRequest {

	private String url;
	private String function;
	private ObjectNode data;
	private Map<String, String> header = new HashMap<String, String>();
	private Map<String, String> params = new HashMap<String, String>();
	private String dbName;

	public TDLuceneRequest() {

	}

	/**
	 * Add a parameter to the request. Forces string input for value
	 * 
	 * @param param
	 * @param value
	 */
	public TDLuceneRequest addParam(String param, String value) {
		this.params.put(param, value);
		return this;
	}

	public String getParameter(String param) {
		return getParamAsString(param);
	}

	public boolean getParamAsBoolean(String param) {
		return Boolean.parseBoolean(params.get(param));
	}

	public String getParamAsString(String param) {
		return params.get(param);
	}

	public TDLuceneRequest addHeader(String param, String value) {
		this.header.put(param, value);
		return this;
	}

	public String getHeader(String param) {
		return this.header.get(param);
	}

	public TDLuceneRequest addData(ObjectNode json) {
		this.data = json;
		return this;
	}

	public ObjectNode getData() {
		return this.data;
	}

	public TDLuceneRequest setUrl(String dbName, String function,
			String ddocName, String index) {
		if ("query".equals(function) || "info".equals(function)) {
			this.url = String.format("/_local/%s/_design/%s/%s", dbName,
					ddocName, index);
		} else if ("optimize".equals(function) || "expunge".equals(function)) {
			this.url = String.format("/_local/%s/_design/%s/%s/_%s", dbName,
					ddocName, index, function);
		}
		this.setDbName(dbName);
		this.function = function;
		return this;
	}

	public String getUrl() {
		return url;
	}

	/**
	 * Provides the url ignoring the params
	 * 
	 * @return
	 */
	public String getRequestURI() {
		return getUrl();
	}

	public TDLuceneRequest setFunction(String function) {
		this.function = function;
		return this;
	}

	public String getFunction() {
		return function;
	}

	public TDLuceneRequest setDbName(String dbName) {
		this.dbName = dbName;
		return this;
	}

	public String getDbName() {
		return dbName;
	}
}
