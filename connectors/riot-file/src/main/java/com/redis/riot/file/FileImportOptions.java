package com.redis.riot.file;

import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;

import picocli.CommandLine.Option;

public class FileImportOptions extends FileOptions {

	public static final String DEFAULT_CONTINUATION_STRING = "\\";

	@Option(names = "--fields", arity = "1..*", description = "Delimited/FW field names", paramLabel = "<names>")
	private String[] names;
	@Option(names = { "-h", "--header" }, description = "Delimited/FW first line contains field names")
	private boolean header;
	@Option(names = "--delimiter", description = "Delimiter character", paramLabel = "<string>")
	private String delimiter;
	@Option(names = "--skip", description = "Delimited/FW lines to skip at start", paramLabel = "<count>")
	private Integer linesToSkip;
	@Option(names = "--include", arity = "1..*", description = "Delimited/FW field indices to include (0-based)", paramLabel = "<index>")
	private int[] includedFields;
	@Option(names = "--ranges", arity = "1..*", description = "Fixed-width column ranges", paramLabel = "<string>")
	private String[] columnRanges;
	@Option(names = "--quote", description = "Escape character for delimited files (default: ${DEFAULT-VALUE})", paramLabel = "<char>")
	private Character quoteCharacter = DelimitedLineTokenizer.DEFAULT_QUOTE_CHARACTER;
	@Option(names = "--cont", description = "Line continuation string (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
	private String continuationString = DEFAULT_CONTINUATION_STRING;

	public String[] getNames() {
		return names;
	}

	public void setNames(String[] names) {
		this.names = names;
	}

	public boolean isHeader() {
		return header;
	}

	public void setHeader(boolean header) {
		this.header = header;
	}

	public String getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

	public Integer getLinesToSkip() {
		return linesToSkip;
	}

	public void setLinesToSkip(Integer linesToSkip) {
		this.linesToSkip = linesToSkip;
	}

	public int[] getIncludedFields() {
		return includedFields;
	}

	public void setIncludedFields(int[] includedFields) {
		this.includedFields = includedFields;
	}

	public String[] getColumnRanges() {
		return columnRanges;
	}

	public void setColumnRanges(String[] columnRanges) {
		this.columnRanges = columnRanges;
	}

	public Character getQuoteCharacter() {
		return quoteCharacter;
	}

	public void setQuoteCharacter(Character quoteCharacter) {
		this.quoteCharacter = quoteCharacter;
	}

	public String getContinuationString() {
		return continuationString;
	}

	public void setContinuationString(String continuationString) {
		this.continuationString = continuationString;
	}

}
