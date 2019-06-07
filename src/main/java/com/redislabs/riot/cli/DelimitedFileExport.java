package com.redislabs.riot.cli;

import java.util.Map;

import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder.DelimitedBuilder;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "csv", description = "Export to a delimited file")
public class DelimitedFileExport extends FlatFileExport {

	@Option(names = "--delimiter", description = "Delimiter used when writing output. (default: ${DEFAULT-VALUE}).")
	private String delimiter = DelimitedLineTokenizer.DELIMITER_COMMA;

	@Override
	protected void configure(FlatFileItemWriterBuilder<Map<String, Object>> builder) {
		builder.name("delimited-file-writer");
		DelimitedBuilder<Map<String, Object>> delimited = builder.delimited();
		delimited.delimiter(delimiter);
		delimited.fieldExtractor(fieldExtractor());
	}

	@Override
	protected String header(String[] names) {
		return String.join(delimiter, names);
	}

}
