package com.redislabs.riot.cli;

import org.springframework.batch.item.file.FlatFileItemReader;

import com.redislabs.riot.file.FlatFileOptions;

import picocli.CommandLine.Option;

public abstract class AbstractFlatFileImportSubCommand extends AbstractFileImportSubCommand {

	@Option(names = "--encoding", description = "Encoding for this input source. (default: ${DEFAULT-VALUE}).", order = 10)
	private String encoding = FlatFileItemReader.DEFAULT_CHARSET;
	@Option(names = "--names", arity = "1..*", description = "Names of the fields in the order they occur within the delimited file.", order = 10)
	private String[] fieldNames;
	@Option(names = "--lines-to-skip", description = "Number of lines to skip at the beginning of reading the file. (default: ${DEFAULT-VALUE}).", order = 10)
	private int linesToSkip = 0;

	protected void setOptions(FlatFileOptions options) {
		options.setNames(fieldNames);
		options.setLinesToSkip(linesToSkip);
		options.setEncoding(encoding);
	}

}
