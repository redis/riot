package com.redislabs.riot.cli.file;

import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.Range;

import lombok.Data;
import lombok.EqualsAndHashCode;
import picocli.CommandLine.Option;

@EqualsAndHashCode(callSuper = true)
public @Data class FileReaderOptions extends FileOptions {

	@Option(names = { "-f",
			"--fields" }, arity = "1..*", description = "Names of the fields as they occur in the file", paramLabel = "<names>")
	private List<String> names = new ArrayList<>();
	@Option(names = { "--skip" }, description = "Lines to skip from the beginning of the file", paramLabel = "<count>")
	private Integer linesToSkip;
	@Option(names = "--include", arity = "1..*", description = "Field indices to include (0-based)", paramLabel = "<index>")
	private List<Integer> includedFields = new ArrayList<>();
	@Option(names = "--ranges", arity = "1..*", description = "Fixed-width column ranges", paramLabel = "<int>")
	private List<Range> columnRanges = new ArrayList<>();
	@Option(names = { "-q",
			"--quote" }, description = "Escape character (default: ${DEFAULT-VALUE})", paramLabel = "<char>")
	private Character quoteCharacter = DelimitedLineTokenizer.DEFAULT_QUOTE_CHARACTER;
	@Option(names = { "-d",
			"--delimiter" }, description = "Delimiter character (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
	private String delimiter = DelimitedLineTokenizer.DELIMITER_COMMA;
	@Option(names = { "-h", "--header" }, description = "Write field names on the first line")
	private boolean header;

}
