package com.redislabs.riot.cli;

import java.io.IOException;
import java.util.Map;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

import com.redislabs.riot.file.FileReaderBuilder;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "fw", description = "Import a fixed-width file")
public class FixedLengthImportSubCommand extends AbstractFlatFileImportSubCommand {

	@Option(names = "--ranges", arity = "1..*", description = "Column ranges.", required = true)
	private String[] columnRanges;

	@Override
	public AbstractItemCountingItemStreamItemReader<Map<String, Object>> reader() throws IOException {
		FileReaderBuilder builder = builder();
		builder.setColumnRanges(columnRanges);
		return builder.buildFixedLength();
	}

}
