package com.redislabs.riot.cli.file;

import org.springframework.batch.item.file.transform.Range;

import picocli.CommandLine.ITypeConverter;

public class RangeConverter implements ITypeConverter<Range> {

	@Override
	public Range convert(String value) throws Exception {
		String[] split = value.split("-");
		return new Range(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
	}

}
