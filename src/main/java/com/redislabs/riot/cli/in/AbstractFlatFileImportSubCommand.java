package com.redislabs.riot.cli.in;

import org.springframework.batch.item.file.FlatFileItemReader;

import com.redislabs.riot.file.FileReaderBuilder;

import picocli.CommandLine.Option;

public abstract class AbstractFlatFileImportSubCommand extends AbstractFileImportSubCommand {

	@Option(names = "--encoding", description = "Encoding for this input source. (default: ${DEFAULT-VALUE}).")
	private String encoding = FlatFileItemReader.DEFAULT_CHARSET;
	@Option(names = "--names", arity = "1..*", description = "Names of the fields in the order they occur within the delimited file.")
	private String[] fieldNames;
	@Option(names = "--lines-to-skip", description = "Number of lines to skip at the beginning of reading the file. (default: ${DEFAULT-VALUE}).")
	private Integer linesToSkip;

	@Override
	protected FileReaderBuilder builder() {
		FileReaderBuilder builder = super.builder();
		if (fieldNames != null) {
			builder.setNames(fieldNames);
		}
		if (linesToSkip != null) {
			builder.setLinesToSkip(linesToSkip);
		}
		if (encoding != null) {
			builder.setEncoding(encoding);
		}
		return builder;
	}

}
