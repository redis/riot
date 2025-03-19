package com.redis.riot;

import com.redis.riot.file.FileOptions;

import lombok.ToString;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

@ToString
public class FileArgs {

	@Option(names = { "-z", "--gzip" }, description = "File is gzip compressed.")
	private boolean gzipped;

	@ArgGroup(exclusive = false)
	private S3Args s3Args = new S3Args();

	@ArgGroup(exclusive = false)
	private GoogleStorageArgs googleStorageArgs = new GoogleStorageArgs();

	@Option(names = "--delimiter", description = "Delimiter character.", paramLabel = "<string>")
	private String delimiter;

	@Option(names = "--encoding", description = "File encoding (default: ${DEFAULT-VALUE}).", paramLabel = "<charset>")
	private String encoding = FileOptions.DEFAULT_ENCODING;

	@Option(names = "--quote", description = "Escape character for CSV files (default: ${DEFAULT-VALUE}).", paramLabel = "<char>")
	private char quoteCharacter = FileOptions.DEFAULT_QUOTE_CHARACTER;

	public S3Args getS3Args() {
		return s3Args;
	}

	public void setS3Args(S3Args args) {
		this.s3Args = args;
	}

	public GoogleStorageArgs getGoogleStorageArgs() {
		return googleStorageArgs;
	}

	public void setGoogleStorageArgs(GoogleStorageArgs gcpArgs) {
		this.googleStorageArgs = gcpArgs;
	}

	public boolean isGzipped() {
		return gzipped;
	}

	public void setGzipped(boolean gzipped) {
		this.gzipped = gzipped;
	}

	public String getEncoding() {
		return encoding;
	}

	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

	public char getQuoteCharacter() {
		return quoteCharacter;
	}

	public void setQuoteCharacter(char character) {
		this.quoteCharacter = character;
	}

	public String getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

	public void apply(FileOptions options) {
		options.setDelimiter(delimiter);
		options.setEncoding(encoding);
		options.setQuoteCharacter(quoteCharacter);
		options.setS3Options(s3Args.s3Options());
		options.setGoogleStorageOptions(googleStorageArgs.googleStorageOptions());
	}

}
