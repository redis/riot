package com.redislabs.recharge.file;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;

import com.redislabs.recharge.KeyConfiguration;
import com.redislabs.recharge.MapItemProcessor;

public abstract class AbstractFlatFileStep {

	@Autowired
	private FileConfiguration fileConfig;
	@Autowired
	private KeyConfiguration keyConfig;
	@Autowired
	private FlatFileConfiguration config;

	protected FlatFileConfiguration getConfig() {
		return config;
	}

	public FlatFileItemReader<Map<String, String>> reader() throws IOException {
		FlatFileItemReaderBuilder<Map<String, String>> builder = new FlatFileItemReaderBuilder<>();
		builder.name("file-reader");
		builder.resource(fileConfig.getResource());
		if (config.getLinesToSkip() != null) {
			builder.linesToSkip(config.getLinesToSkip());
		}
		if (fileConfig.getEncoding() != null) {
			builder.encoding(fileConfig.getEncoding());
		}
		builder.strict(true);
		builder.saveState(false);
		builder.fieldSetMapper(new MapFieldSetMapper());
		configure(builder);
		return builder.build();
	}

	public MapItemProcessor processor() {
		String keyPrefix = keyConfig.getPrefix();
		if (keyPrefix == null) {
			keyPrefix = getBaseFilename();
		}
		String keySeparator = keyConfig.getSeparator();
		String[] keyFields = keyConfig.getFields();
		if (keyConfig.getFields() == null || keyConfig.getFields().length == 0) {
			keyFields = new String[] { config.getFieldNames()[0] };
		}
		return new MapItemProcessor(keyPrefix, keyFields, keySeparator);
	}

	private String getBaseFilename() {
		String filename = new File(fileConfig.getFile()).getName();
		int pos = filename.indexOf(".");
		if (pos == -1) {
			return filename;
		}
		return filename.substring(0, pos);
	}

	abstract protected void configure(FlatFileItemReaderBuilder<Map<String, String>> builder);

}
