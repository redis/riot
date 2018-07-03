package com.redislabs.recharge.file.fixedlength;

import java.util.Map;

import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder.FixedLengthBuilder;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import com.redislabs.recharge.file.AbstractFlatFileStep;

@Configuration
public class FixedLengthFileStep extends AbstractFlatFileStep {

	@Autowired
	private FixedLengthConfiguration fixedLength;

	@Override
	protected void configure(FlatFileItemReaderBuilder<Map<String, String>> builder) {
		FixedLengthBuilder<Map<String, String>> fixedLengthBuilder = builder.fixedLength();
		if (fixedLength.getRanges() != null) {
			fixedLengthBuilder.columns(getRanges(fixedLength.getRanges()));
		}
		if (getConfig().getFieldNames() != null) {
			fixedLengthBuilder.names(getConfig().getFieldNames());
		}
		if (fixedLength.getStrict() != null) {
			fixedLengthBuilder.strict(fixedLength.getStrict());
		}
	}

	private Range[] getRanges(String[] strings) {
		Range[] ranges = new Range[strings.length];
		for (int index = 0; index < strings.length; index++) {
			ranges[index] = getRange(strings[index]);
		}
		return ranges;
	}

	private Range getRange(String string) {
		String[] split = string.split("-");
		return new Range(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
	}

}