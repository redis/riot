package com.redis.riot.file;

import java.util.Map;
import java.util.function.Supplier;

import lombok.ToString;

@ToString
public class WriteOptions extends FileOptions {

	public static final String DEFAULT_LINE_SEPARATOR = System.getProperty("line.separator");
	public static final boolean DEFAULT_SHOULD_DELETE_IF_EXISTS = true;
	public static final boolean DEFAULT_TRANSACTIONAL = true;
	public static final String DEFAULT_ELEMENT_NAME = "record";
	public static final String DEFAULT_ROOT_NAME = "root";

	private String formatterString;
	private boolean append;
	private boolean forceSync;
	private String lineSeparator = DEFAULT_LINE_SEPARATOR;
	private boolean shouldDeleteIfEmpty;
	private boolean shouldDeleteIfExists = DEFAULT_SHOULD_DELETE_IF_EXISTS;
	private boolean transactional = DEFAULT_TRANSACTIONAL;
	private String rootName = DEFAULT_ROOT_NAME;
	private String elementName = DEFAULT_ELEMENT_NAME;
	private Supplier<Map<String, Object>> headerSupplier = () -> null;

	public Supplier<Map<String, Object>> getHeaderSupplier() {
		return headerSupplier;
	}

	public void setHeaderSupplier(Supplier<Map<String, Object>> headerSupplier) {
		this.headerSupplier = headerSupplier;
	}

	public String getRootName() {
		return rootName;
	}

	public void setRootName(String name) {
		this.rootName = name;
	}

	public String getElementName() {
		return elementName;
	}

	public void setElementName(String name) {
		this.elementName = name;
	}

	public boolean isAppend() {
		return append;
	}

	public void setAppend(boolean append) {
		this.append = append;
	}

	public boolean isForceSync() {
		return forceSync;
	}

	public void setForceSync(boolean forceSync) {
		this.forceSync = forceSync;
	}

	public String getLineSeparator() {
		return lineSeparator;
	}

	public void setLineSeparator(String separator) {
		this.lineSeparator = separator;
	}

	public boolean isShouldDeleteIfEmpty() {
		return shouldDeleteIfEmpty;
	}

	public void setShouldDeleteIfEmpty(boolean delete) {
		this.shouldDeleteIfEmpty = delete;
	}

	public boolean isShouldDeleteIfExists() {
		return shouldDeleteIfExists;
	}

	public void setShouldDeleteIfExists(boolean delete) {
		this.shouldDeleteIfExists = delete;
	}

	public String getFormatterString() {
		return formatterString;
	}

	public void setFormatterString(String formatterString) {
		this.formatterString = formatterString;
	}

	public boolean isTransactional() {
		return transactional;
	}

	public void setTransactional(boolean transactional) {
		this.transactional = transactional;
	}

}
