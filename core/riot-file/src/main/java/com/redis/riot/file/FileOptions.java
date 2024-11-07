package com.redis.riot.file;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import lombok.ToString;

@ToString
public class FileOptions {

	public static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();
	public static final char DEFAULT_QUOTE_CHARACTER = '"';

	private ResourceOptions resourceOptions = new ResourceOptions();
	private FileType fileType;
	private String encoding = DEFAULT_ENCODING;
	private boolean header;
	private Optional<String> delimiter = Optional.empty();
	private char quoteCharacter = DEFAULT_QUOTE_CHARACTER;

	public ResourceOptions getResourceOptions() {
		return resourceOptions;
	}

	public void setResourceOptions(ResourceOptions resourceOptions) {
		this.resourceOptions = resourceOptions;
	}

	public FileType getFileType() {
		return fileType;
	}

	public void setFileType(FileType type) {
		this.fileType = type;
	}

	public Optional<String> getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = Optional.ofNullable(delimiter);
	}

	public char getQuoteCharacter() {
		return quoteCharacter;
	}

	public void setQuoteCharacter(char character) {
		this.quoteCharacter = character;
	}

	public boolean isHeader() {
		return header;
	}

	public void setHeader(boolean header) {
		this.header = header;
	}

	public String getEncoding() {
		return encoding;
	}

	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

}
