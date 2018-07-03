package com.redislabs.recharge.file.delimited;

import java.util.Map;

import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder.DelimitedBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import com.redislabs.recharge.file.AbstractFlatFileStep;

@Configuration
public class DelimitedFileStep extends AbstractFlatFileStep {

	@Autowired
	private DelimitedConfiguration config;

	@Override
	protected void configure(FlatFileItemReaderBuilder<Map<String, String>> readerBuilder) {
		DelimitedBuilder<Map<String, String>> delimitedBuilder = readerBuilder.delimited();
		if (config.getDelimiter() != null) {
			delimitedBuilder.delimiter(config.getDelimiter());
		}
		if (config.getIncludedFields() != null) {
			delimitedBuilder.includedFields(config.getIncludedFields());
		}
		if (config.getQuoteCharacter() != null) {
			delimitedBuilder.quoteCharacter(config.getQuoteCharacter());
		}
		if (getConfig().getFieldNames() != null) {
			delimitedBuilder.names(getConfig().getFieldNames());
		}
	}

}