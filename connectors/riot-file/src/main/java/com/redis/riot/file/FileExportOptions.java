package com.redis.riot.file;

import org.springframework.batch.item.support.AbstractFileItemWriter;

import picocli.CommandLine.Option;

public class FileExportOptions extends DumpFileOptions {

	public static final String DEFAULT_ELEMENT_NAME = "record";
	public static final String DEFAULT_ROOT_NAME = "root";

	@Option(names = "--append", description = "Append to file if it exists")
	private boolean append;
	@Option(names = "--root", description = "XML root element tag name (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
	private String rootName = DEFAULT_ROOT_NAME;
	@Option(names = "--element", description = "XML element tag name (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
	private String elementName = DEFAULT_ELEMENT_NAME;
	@Option(names = "--line-sep", description = "String to separate lines (default: system default)", paramLabel = "<string>")
	private String lineSeparator = AbstractFileItemWriter.DEFAULT_LINE_SEPARATOR;

	public boolean isAppend() {
		return append;
	}

	public void setAppend(boolean append) {
		this.append = append;
	}

	public String getRootName() {
		return rootName;
	}

	public void setRootName(String rootName) {
		this.rootName = rootName;
	}

	public String getElementName() {
		return elementName;
	}

	public void setElementName(String elementName) {
		this.elementName = elementName;
	}

	public String getLineSeparator() {
		return lineSeparator;
	}

	public void setLineSeparator(String lineSeparator) {
		this.lineSeparator = lineSeparator;
	}

	@Override
	public String toString() {
		return "FileExportOptions [type=" + type + ", append=" + append + ", rootName=" + rootName + ", elementName="
				+ elementName + ", lineSeparator=" + lineSeparator + ", encoding=" + encoding + ", gzip=" + gzip
				+ ", s3=" + s3 + ", gcs=" + gcs + "]";
	}

}
