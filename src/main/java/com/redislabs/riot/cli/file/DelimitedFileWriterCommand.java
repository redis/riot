package com.redislabs.riot.cli.file;

import java.util.Map;

import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder.DelimitedBuilder;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "csv", description = "Delimited file")
public class DelimitedFileWriterCommand extends AbstractFlatFileWriterCommand {

	@Option(names = "--delimiter", description = "Delimiter used when writing output.")
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
