package com.redis.riot;

import com.redis.riot.file.FileOptions;

import lombok.ToString;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

@ToString
public class FileArgs {

	@ArgGroup(exclusive = false)
	private ResourceArgs resourceArgs = new ResourceArgs();

	@Option(names = "--delimiter", description = "Delimiter character.", paramLabel = "<string>")
	private String delimiter;

	@Option(names = "--encoding", description = "File encoding (default: ${DEFAULT-VALUE}).", paramLabel = "<charset>")
	private String encoding = FileOptions.DEFAULT_ENCODING;

	@Option(names = "--header", description = "Use first line as field names for CSV/fixed-length files")
	private boolean header;

	@Option(names = "--quote", description = "Escape character for CSV files (default: ${DEFAULT-VALUE}).", paramLabel = "<char>")
	private char quoteCharacter = FileOptions.DEFAULT_QUOTE_CHARACTER;

	public ResourceArgs getResourceArgs() {
		return resourceArgs;
	}

	public void setResourceArgs(ResourceArgs resourceArgs) {
		this.resourceArgs = resourceArgs;
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

	public boolean isHeader() {
		return header;
	}

	public void setHeader(boolean header) {
		this.header = header;
	}

	public FileOptions fileOptions() {
		FileOptions options = new FileOptions();
		options.setResourceOptions(resourceArgs.resourceOptions());
		options.setDelimiter(delimiter);
		options.setEncoding(encoding);
		options.setHeader(header);
		options.setQuoteCharacter(quoteCharacter);
		return options;
	}

}
