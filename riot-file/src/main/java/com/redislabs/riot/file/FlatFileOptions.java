package com.redislabs.riot.file;

import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.Range;

import lombok.Getter;
import picocli.CommandLine.Option;

public class FlatFileOptions {

	@Getter
	@Option(names = "--fields", arity = "1..*", description = "Delimited/FW field names", paramLabel = "<names>")
	private String[] names;
	@Option(names = { "-h", "--header" }, description = "Delimited/FW first line contains field names")
	private boolean header;
	@Option(names = "--delimiter", description = "Delimiter character", paramLabel = "<string>")
	private String delimiter;
	@Option(names = "--skip", description = "Delimited/FW lines to skip at start", paramLabel = "<count>")
	private Integer linesToSkip;
	@Getter
	@Option(names = "--include", arity = "1..*", description = "Delimited/FW field indices to include (0-based)", paramLabel = "<index>")
	private int[] includedFields = new int[0];
	@Getter
	@Option(names = "--ranges", arity = "1..*", description = "Fixed-width column ranges", paramLabel = "<int>")
	private Range[] columnRanges = new Range[0];
	@Getter
	@Option(names = "--quote", description = "Escape character for delimited files (default: ${DEFAULT-VALUE})", paramLabel = "<char>")
	private Character quoteCharacter = DelimitedLineTokenizer.DEFAULT_QUOTE_CHARACTER;

	public String delimiter(String file) {
		if (delimiter == null) {
			String extension = FileOptions.extension(file);
			if (extension != null) {
				switch (extension) {
				case FileOptions.EXT_TSV:
					return DelimitedLineTokenizer.DELIMITER_TAB;
				case FileOptions.EXT_CSV:
					return DelimitedLineTokenizer.DELIMITER_COMMA;
				}
			}
			return DelimitedLineTokenizer.DELIMITER_COMMA;
		}
		return delimiter;
	}

	public int getLinesToSkip() {
		if (linesToSkip == null) {
			if (header) {
				return 1;
			}
			return 0;
		}
		return linesToSkip;
	}

}
