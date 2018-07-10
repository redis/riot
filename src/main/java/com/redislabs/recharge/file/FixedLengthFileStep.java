package com.redislabs.recharge.file;

import java.util.Map;

import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder.FixedLengthBuilder;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.stereotype.Component;

import com.redislabs.recharge.config.FixedLengthConfiguration;
import com.redislabs.recharge.config.FlatFileConfiguration;

@Component
public class FixedLengthFileStep extends AbstractFlatFileStep {

	@Override
	protected void configure(FlatFileItemReaderBuilder<Map<String, Object>> builder,
			FlatFileConfiguration flatFileConfig) {
		FixedLengthConfiguration config = flatFileConfig.getFixedLength();
		FixedLengthBuilder<Map<String, Object>> fixedLengthBuilder = builder.fixedLength();
		if (config.getRanges() != null) {
			fixedLengthBuilder.columns(getRanges(config.getRanges()));
		}
		if (config.getStrict() != null) {
			fixedLengthBuilder.strict(config.getStrict());
		}
		if (getFieldNames() != null) {
			fixedLengthBuilder.names(getFieldNames());
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