package com.redis.riot.file;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.util.ResourceUtils;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class FileArgs {

	public static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();
	public static final char DEFAULT_QUOTE_CHARACTER = '"';

	@ArgGroup(exclusive = false)
	private AwsArgs amazonS3Args = new AwsArgs();

	@ArgGroup(exclusive = false)
	private GoogleStorageArgs googleStorageArgs = new GoogleStorageArgs();

	@Option(names = "--delimiter", description = "Delimiter character.", paramLabel = "<string>")
	private String delimiter;

	@Option(names = "--encoding", description = "File encoding (default: ${DEFAULT-VALUE}).", paramLabel = "<charset>")
	private String encoding = DEFAULT_ENCODING;

	@Option(names = { "-t", "--filetype" }, description = "File type: ${COMPLETION-CANDIDATES}.", paramLabel = "<type>")
	private FileType fileType;

	@Option(names = { "-z", "--gzip" }, description = "File is gzip compressed.")
	private boolean gzipped;

	@Option(names = "--header", description = "Use first line as field names for CSV/fixed-length files")
	private boolean header;

	@Option(names = "--quote", description = "Escape character for CSV files (default: ${DEFAULT-VALUE}).", paramLabel = "<char>")
	private char quoteCharacter = DEFAULT_QUOTE_CHARACTER;

	public FileType fileType(Resource resource) {
		if (fileType == null) {
			FileType type = FileUtils.fileType(resource);
			if (type == null) {
				return FileType.JSONL;
			}
			return type;
		}
		return fileType;
	}

	public Resource resource(String location) throws IOException {
		if (AwsArgs.isSimpleStorageResource(location)) {
			return amazonS3Args.resource(location);
		}
		if (GoogleStorageArgs.isGoogleStorageResource(location)) {
			return googleStorageArgs.resource(location);
		}
		if (ResourceUtils.isUrl(location)) {
			return new UncustomizedUrlResource(location);
		}
		return new FileSystemResource(location);
	}

	public FileType fileType(String file) throws IOException {
		return fileType(resource(file));
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

	public GoogleStorageArgs getGoogleStorageArgs() {
		return googleStorageArgs;
	}

	public void setGoogleStorageArgs(GoogleStorageArgs args) {
		this.googleStorageArgs = args;
	}

	public AwsArgs getAmazonS3Args() {
		return amazonS3Args;
	}

	public void setAmazonS3Args(AwsArgs args) {
		this.amazonS3Args = args;
	}

	public FileType getFileType() {
		return fileType;
	}

	public void setFileType(FileType type) {
		this.fileType = type;
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

	public boolean isHeader() {
		return header;
	}

	public void setHeader(boolean header) {
		this.header = header;
	}

	@Override
	public String toString() {
		return "amazonS3Args=" + amazonS3Args + ", googleStorageArgs=" + googleStorageArgs + ", delimiter=" + delimiter
				+ ", encoding=" + encoding + ", fileType=" + fileType + ", gzipped=" + gzipped + ", header=" + header
				+ ", quoteCharacter=" + quoteCharacter;
	}

}
