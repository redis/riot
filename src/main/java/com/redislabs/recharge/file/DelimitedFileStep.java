package com.redislabs.recharge.file;

import java.util.Map;

import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder.DelimitedBuilder;
import org.springframework.context.annotation.Configuration;

import com.redislabs.recharge.config.DelimitedConfiguration;
import com.redislabs.recharge.config.FlatFileConfiguration;

@Configuration
public class DelimitedFileStep extends AbstractFlatFileStep {

	@Override
	protected void configure(FlatFileItemReaderBuilder<Map<String, Object>> readerBuilder,
			FlatFileConfiguration flatFileConfig) {
		DelimitedConfiguration config = flatFileConfig.getDelimited();
		DelimitedBuilder<Map<String, Object>> delimitedBuilder = readerBuilder.delimited();
		if (config.getDelimiter() != null) {
			delimitedBuilder.delimiter(config.getDelimiter());
		}
		if (config.getIncludedFields() != null) {
			delimitedBuilder.includedFields(config.getIncludedFields());
		}
		if (config.getQuoteCharacter() != null) {
			delimitedBuilder.quoteCharacter(config.getQuoteCharacter());
		}
		if (getFieldNames() != null) {
			delimitedBuilder.names(getFieldNames());
		}
	}

}