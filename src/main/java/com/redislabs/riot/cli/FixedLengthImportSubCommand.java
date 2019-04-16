package com.redislabs.riot.cli;

import java.io.IOException;
import java.util.Map;

import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.core.io.Resource;

import com.redislabs.riot.file.FileConfig;
import com.redislabs.riot.file.FixedLengthFileOptions;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "fw", description = "Import a fixed-width file", sortOptions = false)
public class FixedLengthImportSubCommand extends AbstractFlatFileImportSubCommand {

	@Option(arity = "1..*", names = "--ranges", description = "Column ranges.", order = 6)
	private String[] ranges;

	@Override
	protected FlatFileItemReader<Map<String, Object>> reader(Resource resource) throws IOException {
		FixedLengthFileOptions options = new FixedLengthFileOptions();
		options.setRanges(ranges);
		setOptions(options);
		return new FileConfig().reader(resource, options);
	}

}
