package com.redis.riot.file;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.util.Assert;

import com.google.cloud.spring.core.GcpScope;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class FileWriterArgs {

	public static final String DEFAULT_LINE_SEPARATOR = System.getProperty("line.separator");
	public static final String DEFAULT_ELEMENT_NAME = "record";
	public static final String DEFAULT_ROOT_NAME = "root";
	public static final boolean DEFAULT_SHOULD_DELETE_IF_EXISTS = true;
	public static final boolean DEFAULT_TRANSACTIONAL = true;

	@Option(names = "--format", description = "Format string used to aggregate items.", hidden = true)
	private String formatterString;

	@Option(names = "--append", description = "Append to file if it exists.")
	private boolean append;

	@Option(names = "--force-sync", description = "Force-sync changes to disk on flush.", hidden = true)
	private boolean forceSync;

	@Option(names = "--root", description = "XML root element tag name (default: ${DEFAULT-VALUE}).", paramLabel = "<string>")
	private String rootName = DEFAULT_ROOT_NAME;

	@Option(names = "--element", description = "XML element tag name (default: ${DEFAULT-VALUE}).", paramLabel = "<string>")
	private String elementName = DEFAULT_ELEMENT_NAME;

	@Option(names = "--line-sep", description = "String to separate lines (default: system default).", paramLabel = "<string>")
	private String lineSeparator = DEFAULT_LINE_SEPARATOR;

	@Option(names = "--delete-empty", description = "Delete file if still empty after export.")
	private boolean shouldDeleteIfEmpty;

	@Option(names = "--delete-exists", description = "Delete file if it already exists.", negatable = true, defaultValue = "true", fallbackValue = "true")
	private boolean shouldDeleteIfExists = DEFAULT_SHOULD_DELETE_IF_EXISTS;

	@Option(names = "--transactional", description = "Delay writing to the buffer if a transaction is active.", negatable = true, defaultValue = "true", fallbackValue = "true")
	private boolean transactional = DEFAULT_TRANSACTIONAL;

	@ArgGroup(exclusive = false)
	private FileArgs fileArgs = defaultFileArgs();

	public static FileArgs defaultFileArgs() {
		FileArgs fileArgs = new FileArgs();
		fileArgs.getGoogleStorageArgs().setScope(GcpScope.STORAGE_READ_WRITE);
		return fileArgs;
	}

	public WritableResource resource(String location) throws IOException {
		if (location == null) {
			return new SystemOutResource();
		}
		Resource resource = fileArgs.resource(location);
		Assert.notNull(resource, "Could not resolve file " + location);
		Assert.isInstanceOf(WritableResource.class, resource);
		WritableResource writableResource = (WritableResource) resource;
		if (fileArgs.isGzipped() || FileUtils.isGzip(location)) {
			OutputStream outputStream = writableResource.getOutputStream();
			return new OutputStreamResource(new GZIPOutputStream(outputStream), resource.getFilename(),
					resource.getDescription());
		}
		return writableResource;
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

	@Override
	public String toString() {
		return "FileWriterArgs [formatterString=" + formatterString + ", append=" + append + ", forceSync=" + forceSync
				+ ", rootName=" + rootName + ", elementName=" + elementName + ", lineSeparator=" + lineSeparator
				+ ", shouldDeleteIfEmpty=" + shouldDeleteIfEmpty + ", shouldDeleteIfExists=" + shouldDeleteIfExists
				+ ", transactional=" + transactional + ", fileArgs=" + fileArgs + "]";
	}

}
