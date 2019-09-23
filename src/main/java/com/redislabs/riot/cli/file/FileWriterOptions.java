package com.redislabs.riot.cli.file;

import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.Range;

import picocli.CommandLine.Option;

public class FileWriterOptions {

	@Option(names = { "-f",
			"--fields" }, arity = "1..*", description = "Names of the fields as they occur in the file", paramLabel = "<names>")
	private String[] names = new String[0];
	@Option(names = { "--skip" }, description = "Lines to skip from the beginning of the file", paramLabel = "<count>")
	private Integer linesToSkip;
	@Option(names = "--include", arity = "1..*", description = "Field indices to include (0-based)", paramLabel = "<index>")
	private Integer[] includedFields = new Integer[0];
	@Option(names = "--ranges", arity = "1..*", description = "Fixed-width column ranges", paramLabel = "<int>")
	private Range[] columnRanges = new Range[0];
	@Option(names = { "-q",
			"--quote" }, description = "Escape character (default: ${DEFAULT-VALUE})", paramLabel = "<char>")
	private Character quoteCharacter = DelimitedLineTokenizer.DEFAULT_QUOTE_CHARACTER;

	public Integer getLinesToSkip() {
		return linesToSkip;
	}

	public String[] getNames() {
		return names;
	}

	public Integer[] getIncludedFields() {
		return includedFields;
	}

	public Range[] getColumnRanges() {
		return columnRanges;
	}

	public Character getQuoteCharacter() {
		return quoteCharacter;
	}

}
