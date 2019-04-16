package com.redislabs.riot.cli;

import java.io.IOException;
import java.util.Map;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

import com.redislabs.riot.file.FileReaderBuilder;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "fw", description = "Import a fixed-width file", sortOptions = false)
public class FixedLengthImportSubCommand extends AbstractFlatFileImportSubCommand {

	@Option(arity = "1..*", names = "--ranges", description = "Column ranges.", required = true, order = 6)
	private String[] columnRanges;

	@Override
	protected AbstractItemCountingItemStreamItemReader<Map<String, Object>> countingReader() throws IOException {
		FileReaderBuilder builder = builder();
		builder.setColumnRanges(columnRanges);
		return builder.buildFixedLength();
	}

}
