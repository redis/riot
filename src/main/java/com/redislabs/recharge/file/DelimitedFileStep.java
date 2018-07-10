package com.redislabs.recharge.file;

import java.util.Map;

import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder.DelimitedBuilder;
import org.springframework.context.annotation.Configuration;

import com.redislabs.recharge.RechargeConfiguration.DelimitedConfiguration;
import com.redislabs.recharge.RechargeConfiguration.FileConfiguration;
import com.redislabs.recharge.RechargeConfiguration.FlatFileConfiguration;

@Configuration
public class DelimitedFileStep extends AbstractFlatFileStep {

	@Override
	protected FlatFileConfiguration configure(FlatFileItemReaderBuilder<Map<String, Object>> builder,
			FileConfiguration fileConfig) {
		DelimitedConfiguration config = fileConfig.getDelimited();
		DelimitedBuilder<Map<String, Object>> delimitedBuilder = builder.delimited();
		if (config.getDelimiter() != null) {
			delimitedBuilder.delimiter(config.getDelimiter());
		}
		if (config.getIncludedFields() != null) {
			delimitedBuilder.includedFields(config.getIncludedFields());
		}
		if (config.getQuoteCharacter() != null) {
			delimitedBuilder.quoteCharacter(config.getQuoteCharacter());
		}
		if (config.getFields() != null) {
			delimitedBuilder.names(config.getFields());
		}
		return config;
	}

}