package com.redis.riot.file;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import org.springframework.cloud.gcp.core.GcpScope;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.util.Assert;

import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

public class FileWriterArgs extends FileArgs {

	public static final String DEFAULT_LINE_SEPARATOR = System.getProperty("line.separator");
	public static final String DEFAULT_ELEMENT_NAME = "record";
	public static final String DEFAULT_ROOT_NAME = "root";
	public static final boolean DEFAULT_SHOULD_DELETE_IF_EXISTS = true;
	public static final boolean DEFAULT_TRANSACTIONAL = true;

	@Parameters(arity = "0..1", description = "File path or URL. If omitted, export is written to stdout.", paramLabel = "FILE")
	private String file;

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

	public FileWriterArgs() {
		getGoogleStorageArgs().setScope(GcpScope.STORAGE_READ_WRITE);
	}

	@Override
	public WritableResource resource(String location) {
		if (location == null) {
			return new SystemOutResource();
		}
		Resource resource;
		try {
			resource = super.resource(location);
		} catch (IOException e) {
			throw new RuntimeIOException("Could not get resource " + location, e);
		}
		Assert.notNull(resource, "Could not resolve file " + location);
		Assert.isInstanceOf(WritableResource.class, resource);
		WritableResource writableResource = (WritableResource) resource;
		if (isGzipped() || FileUtils.isGzip(location)) {
			OutputStream outputStream;
			try {
				outputStream = writableResource.getOutputStream();
			} catch (IOException e) {
				throw new RuntimeIOException("Could not open output stream on resource " + writableResource, e);
			}
			GZIPOutputStream gzipOutputStream;
			try {
				gzipOutputStream = new GZIPOutputStream(outputStream);
			} catch (IOException e) {
				throw new RuntimeIOException("Could not open gzip output stream on resource " + writableResource, e);
			}
			return new OutputStreamResource(gzipOutputStream, resource.getFilename(), resource.getDescription());
		}
		return writableResource;
	}

	public String getFile() {
		return file;
	}

	public void setFile(String file) {
		this.file = file;
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

	public FileType fileType() {
		try {
			return fileType(file);
		} catch (IOException e) {
			throw new IllegalArgumentException("Could not determine type of file " + file, e);
		}
	}

}
