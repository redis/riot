package com.redis.riot.file;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.transform.PassThroughFieldExtractor;
import org.springframework.core.io.WritableResource;

import com.redis.spring.batch.resource.FlatFileItemWriterBuilder;
import com.redis.spring.batch.resource.FlatFileItemWriterBuilder.FormattedBuilder;

public class FormattedWriterFactory extends AbstractWriterFactory {

	@Override
	public ItemWriter<?> create(WritableResource resource, WriteOptions options) {
		FlatFileItemWriterBuilder<Map<String, Object>> writer = flatFileWriter(resource, options);
		FormattedBuilder<Map<String, Object>> formattedBuilder = writer.formatted();
		formattedBuilder.format(options.getFormatterString());
		formattedBuilder.fieldExtractor(new PassThroughFieldExtractor<>());
		return flatFileWriter(options, writer, formattedBuilder.build());
	}

}
