package com.redis.riot;

import com.redis.riot.file.FileWriterOptions;

import lombok.ToString;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

@ToString
public class FileWriterArgs {

	@Option(names = "--format", description = "Format string used to aggregate items.", hidden = true)
	private String formatterString;

	@Option(names = "--append", description = "Append to file if it exists.")
	private boolean append;

	@Option(names = "--force-sync", description = "Force-sync changes to disk on flush.", hidden = true)
	private boolean forceSync;

	@Option(names = "--root", description = "XML root element tag name (default: ${DEFAULT-VALUE}).", paramLabel = "<string>")
	private String rootName = FileWriterOptions.DEFAULT_ROOT_NAME;

	@Option(names = "--element", description = "XML element tag name (default: ${DEFAULT-VALUE}).", paramLabel = "<string>")
	private String elementName = FileWriterOptions.DEFAULT_ELEMENT_NAME;

	@Option(names = "--line-sep", description = "String to separate lines (default: system default).", paramLabel = "<string>")
	private String lineSeparator = FileWriterOptions.DEFAULT_LINE_SEPARATOR;

	@Option(names = "--delete-empty", description = "Delete file if still empty after export.")
	private boolean shouldDeleteIfEmpty;

	@Option(names = "--delete-exists", description = "Delete file if it already exists. True by default.", negatable = true, defaultValue = "true", fallbackValue = "true")
	private boolean shouldDeleteIfExists = FileWriterOptions.DEFAULT_SHOULD_DELETE_IF_EXISTS;

	@Option(names = "--transactional", description = "Delay writing to the buffer if a transaction is active. True by default.", negatable = true, defaultValue = "true", fallbackValue = "true")
	private boolean transactional = FileWriterOptions.DEFAULT_TRANSACTIONAL;

	@ArgGroup(exclusive = false)
	private FileArgs fileArgs = new FileArgs();

	public FileWriterOptions fileWriterOptions() {
		FileWriterOptions options = new FileWriterOptions();
		options.setAppend(append);
		options.setElementName(elementName);
		options.setFileOptions(fileArgs.fileOptions());
		options.setForceSync(forceSync);
		options.setFormatterString(formatterString);
		options.setLineSeparator(lineSeparator);
		options.setRootName(rootName);
		options.setShouldDeleteIfEmpty(shouldDeleteIfEmpty);
		options.setShouldDeleteIfExists(shouldDeleteIfExists);
		options.setTransactional(transactional);
		return options;
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

	public FileArgs getFileArgs() {
		return fileArgs;
	}

	public void setFileArgs(FileArgs fileArgs) {
		this.fileArgs = fileArgs;
	}

}
