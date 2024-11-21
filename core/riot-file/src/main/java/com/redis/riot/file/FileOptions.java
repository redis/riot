package com.redis.riot.file;

import java.nio.charset.StandardCharsets;

import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.util.MimeType;

import lombok.ToString;

@ToString
public class FileOptions {

	public static final String DELIMITER_PIPE = "|";
	public static final String DELIMITER_COMMA = DelimitedLineTokenizer.DELIMITER_COMMA;
	public static final String DELIMITER_TAB = DelimitedLineTokenizer.DELIMITER_TAB;
	public static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();
	public static final char DEFAULT_QUOTE_CHARACTER = '"';

	private boolean gzip;
	private S3Options s3Options = new S3Options();
	private GoogleStorageOptions googleStorageOptions = new GoogleStorageOptions();
	private MimeType contentType;
	private String encoding = DEFAULT_ENCODING;
	private boolean header;
	private String delimiter;
	private char quoteCharacter = DEFAULT_QUOTE_CHARACTER;

	public boolean isGzip() {
		return gzip;
	}

	public void setGzip(boolean gzip) {
		this.gzip = gzip;
	}

	public GoogleStorageOptions getGoogleStorageOptions() {
		return googleStorageOptions;
	}

	public S3Options getS3Options() {
		return s3Options;
	}

	public void setS3Options(S3Options s3Options) {
		this.s3Options = s3Options;
	}

	public void setGoogleStorageOptions(GoogleStorageOptions googleStorageOptions) {
		this.googleStorageOptions = googleStorageOptions;
	}

	public MimeType getContentType() {
		return contentType;
	}

	public void setContentType(MimeType type) {
		this.contentType = type;
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

}
