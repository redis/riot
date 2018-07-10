package com.redislabs.recharge.file;

import java.io.IOException;
import java.util.Map;

import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;

import com.redislabs.recharge.config.FlatFileConfiguration;
import com.redislabs.recharge.config.RechargeConfiguration;

public abstract class AbstractFlatFileStep {

	@Autowired
	private RechargeConfiguration config;

	public FlatFileItemReader<Map<String, Object>> reader() throws IOException {
		FlatFileItemReaderBuilder<Map<String, Object>> builder = new FlatFileItemReaderBuilder<>();
		builder.name("file-reader");
		Resource resource = config.getFile().getResource();
		if (config.getKey().getPrefix() == null) {
			config.getKey().setPrefix(getBaseFilename(resource));
		}
		builder.resource(resource);
		if (config.getFile().getFlat().getLinesToSkip() != null) {
			builder.linesToSkip(config.getFile().getFlat().getLinesToSkip());
		}
		if (config.getFile().getEncoding() != null) {
			builder.encoding(config.getFile().getEncoding());
		}
		builder.strict(true);
		builder.saveState(false);
		builder.fieldSetMapper(new MapFieldSetMapper());
		configure(builder, config.getFile().getFlat());
		return builder.build();
	}

	private String getBaseFilename(Resource resource) {
		String filename = resource.getFilename();
		int pos = filename.indexOf(".");
		if (pos == -1) {
			return filename;
		}
		return filename.substring(0, pos);
	}

	protected String[] getFieldNames() {
		return config.getFile().getFlat().getFields();
	}

	abstract protected void configure(FlatFileItemReaderBuilder<Map<String, Object>> builder,
			FlatFileConfiguration config);

}
