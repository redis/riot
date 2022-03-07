package com.redis.riot.file;

import java.util.Optional;

import org.springframework.batch.item.support.AbstractFileItemWriter;

import picocli.CommandLine;

public class FileExportOptions extends FileOptions {

	public static final String DEFAULT_ELEMENT_NAME = "record";
	public static final String DEFAULT_ROOT_NAME = "root";

	@CommandLine.Option(names = { "-t",
			"--filetype" }, description = "File type: ${COMPLETION-CANDIDATES}", paramLabel = "<type>")
	private Optional<DumpFileType> type = Optional.empty();
	@CommandLine.Option(names = "--append", description = "Append to file if it exists")
	private boolean append;
	@CommandLine.Option(names = "--root", description = "XML root element tag name (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
	private String rootName = DEFAULT_ROOT_NAME;
	@CommandLine.Option(names = "--element", description = "XML element tag name (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
	private String elementName = DEFAULT_ELEMENT_NAME;
	@CommandLine.Option(names = "--line-sep", description = "String to separate lines (default: system default)", paramLabel = "<string>")
	private String lineSeparator = AbstractFileItemWriter.DEFAULT_LINE_SEPARATOR;

	public Optional<DumpFileType> getType() {
		return type;
	}

	public void setType(DumpFileType type) {
		this.type = Optional.of(type);
	}

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

}
