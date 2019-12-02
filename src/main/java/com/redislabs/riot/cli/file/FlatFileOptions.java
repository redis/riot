package com.redislabs.riot.cli.file;

import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;
import picocli.CommandLine.Option;

@EqualsAndHashCode(callSuper = true)
@Accessors(fluent = true)
public @Data class FlatFileOptions extends FileOptions {

	@Option(names = { "-f", "--fields" }, arity = "1..*", description = "Field names", paramLabel = "<names>")
	private String[] names = new String[0];
	@Option(names = { "-e",
			"--encoding" }, description = "File encoding (default: ${DEFAULT-VALUE})", paramLabel = "<charset>")
	private String encoding = FlatFileItemReader.DEFAULT_CHARSET;
	@Option(names = { "-h", "--header" }, description = "First line contains field names")
	private boolean header;
	@Option(names = { "-d",
			"--delimiter" }, description = "Delimiter character (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
	private String delimiter = DelimitedLineTokenizer.DELIMITER_COMMA;

}
