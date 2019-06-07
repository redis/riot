package com.redislabs.riot.cli.in.file;

import java.io.IOException;
import java.util.Map;

import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder.FixedLengthBuilder;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "fw", description = "Import a fixed-width file")
public class FixedLengthFileImport extends AbstractFlatFileImport {

	@Option(names = "--ranges", arity = "1..*", description = "Column ranges.", required = true)
	private String[] columnRanges;

	@Override
	public AbstractItemCountingItemStreamItemReader<Map<String, Object>> reader() throws IOException {
		FlatFileItemReaderBuilder<Map<String, Object>> builder = builder();
		builder.name("fixed-length-file-reader");
		FixedLengthBuilder<Map<String, Object>> fixedlength = builder.fixedLength();
		Assert.notNull(columnRanges, "Column ranges are required");
		Range[] ranges = new Range[columnRanges.length];
		for (int index = 0; index < columnRanges.length; index++) {
			String[] split = columnRanges[index].split("-");
			ranges[index] = new Range(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
		}
		fixedlength.columns(ranges);
		fixedlength.names(getNames());
		return builder.build();
	}

}
