package com.redis.riot;

import com.google.cloud.spring.core.GcpScope;
import com.redis.riot.file.WriteOptions;

import lombok.ToString;
import picocli.CommandLine.Option;

@ToString
public class FileWriterArgs extends FileArgs {

	public static final boolean DEFAULT_HEADER = true;

	@Option(names = "--header", description = "Write first line with field names for CSV/fixed-length files")
	private boolean header = DEFAULT_HEADER;

	@Option(names = "--format", description = "Format string used to aggregate items.", hidden = true)
	private String formatterString;

	@Option(names = "--append", description = "Append to file if it exists.")
	private boolean append;

	@Option(names = "--force-sync", description = "Force-sync changes to disk on flush.", hidden = true)
	private boolean forceSync;

	@Option(names = "--root", description = "XML root element tag name (default: ${DEFAULT-VALUE}).", paramLabel = "<string>")
	private String rootName = WriteOptions.DEFAULT_ROOT_NAME;

	@Option(names = "--element", description = "XML element tag name (default: ${DEFAULT-VALUE}).", paramLabel = "<string>")
	private String elementName = WriteOptions.DEFAULT_ELEMENT_NAME;

	@Option(names = "--line-sep", description = "String to separate lines (default: system default).", paramLabel = "<string>")
	private String lineSeparator = WriteOptions.DEFAULT_LINE_SEPARATOR;

	@Option(names = "--delete-empty", description = "Delete file if still empty after export.")
	private boolean shouldDeleteIfEmpty;

	@Option(names = "--delete-exists", description = "Delete file if it already exists. True by default.", negatable = true, defaultValue = "true", fallbackValue = "true")
	private boolean shouldDeleteIfExists = WriteOptions.DEFAULT_SHOULD_DELETE_IF_EXISTS;

	@Option(names = "--transactional", description = "Delay writing to the buffer if a transaction is active. True by default.", negatable = true, defaultValue = "true", fallbackValue = "true")
	private boolean transactional = WriteOptions.DEFAULT_TRANSACTIONAL;

	public WriteOptions fileWriterOptions() {
		WriteOptions options = new WriteOptions();
		apply(options);
		options.setHeader(header);
		options.getGoogleStorageOptions().setScope(GcpScope.STORAGE_READ_WRITE);
		options.setAppend(append);
		options.setElementName(elementName);
		options.setForceSync(forceSync);
		options.setFormatterString(formatterString);
		options.setLineSeparator(lineSeparator);
		options.setRootName(rootName);
		options.setShouldDeleteIfEmpty(shouldDeleteIfEmpty);
		options.setShouldDeleteIfExists(shouldDeleteIfExists);
		options.setTransactional(transactional);
		return options;
	}

	public boolean isHeader() {
		return header;
	}

	public void setHeader(boolean header) {
		this.header = header;
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
