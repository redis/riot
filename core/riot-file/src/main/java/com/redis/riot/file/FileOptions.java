package com.redis.riot.file;

import java.nio.charset.StandardCharsets;

import org.springframework.util.MimeType;

import lombok.ToString;

@ToString
public class FileOptions {

	public static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();
	public static final char DEFAULT_QUOTE_CHARACTER = '"';

	private MimeType type;
	private S3Options s3Options = new S3Options();
	private GoogleStorageOptions googleStorageOptions = new GoogleStorageOptions();
	private boolean gzipped;
	private String encoding = DEFAULT_ENCODING;
	private boolean header;
	private String delimiter;
	private char quoteCharacter = DEFAULT_QUOTE_CHARACTER;

	public S3Options getS3Options() {
		return s3Options;
	}

	public void setS3Options(S3Options s3Options) {
		this.s3Options = s3Options;
	}

	public GoogleStorageOptions getGoogleStorageOptions() {
		return googleStorageOptions;
	}

	public void setGoogleStorageOptions(GoogleStorageOptions googleStorageOptions) {
		this.googleStorageOptions = googleStorageOptions;
	}

	public MimeType getType() {
		return type;
	}

	public void setType(MimeType type) {
		this.type = type;
	}

	public String getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
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

	public boolean isGzipped() {
		return gzipped;
	}

	public void setGzipped(boolean gzipped) {
		this.gzipped = gzipped;
	}

}
