package com.redis.riot.file;

import java.util.Optional;

import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;

import picocli.CommandLine.Option;

public class FileImportOptions extends FileOptions {

	public static final String DEFAULT_CONTINUATION_STRING = "\\";
	private static final String DELIMITER_PIPE = "|";

	@Option(names = "--fields", arity = "1..*", description = "Delimited/FW field names", paramLabel = "<names>")
	private String[] names;
	@Option(names = { "-h", "--header" }, description = "Delimited/FW first line contains field names")
	private boolean header;
	@Option(names = "--delimiter", description = "Delimiter character", paramLabel = "<string>")
	private Optional<String> delimiter = Optional.empty();
	@Option(names = "--skip", description = "Delimited/FW lines to skip at start", paramLabel = "<count>")
	private Optional<Integer> linesToSkip = Optional.empty();
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

	public void setDelimiter(String delimiter) {
		this.delimiter = Optional.of(delimiter);
	}

	public void setLinesToSkip(int linesToSkip) {
		this.linesToSkip = Optional.of(linesToSkip);
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

	public int getLinesToSkip() {
		if (linesToSkip.isPresent()) {
			return linesToSkip.get();
		}
		if (header) {
			return 1;
		}
		return 0;
	}

	public String delimiter(Optional<String> extension) {
		if (delimiter.isPresent()) {
			return delimiter.get();
		}
		if (extension.isEmpty()) {
			throw new IllegalArgumentException("Could not determine delimiter for extension " + extension);
		}
		switch (extension.get().toLowerCase()) {
		case FileUtils.EXTENSION_CSV:
			return DelimitedLineTokenizer.DELIMITER_COMMA;
		case FileUtils.EXTENSION_PSV:
			return DELIMITER_PIPE;
		case FileUtils.EXTENSION_TSV:
			return DelimitedLineTokenizer.DELIMITER_TAB;
		default:
			throw new IllegalArgumentException("Unknown extension: " + extension);
		}
	}

}
