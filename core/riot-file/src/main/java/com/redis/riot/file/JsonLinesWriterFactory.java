package com.redis.riot.file;

import org.springframework.batch.item.ItemWriter;
import org.springframework.core.io.WritableResource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.spring.batch.resource.FlatFileItemWriterBuilder;

public class JsonLinesWriterFactory extends AbstractWriterFactory {

	@Override
	public ItemWriter<?> create(WritableResource resource, WriteOptions options) {
		FlatFileItemWriterBuilder<?> builder = flatFileWriter(resource, options);
		builder.lineAggregator(new JsonLineAggregator<>(new ObjectMapper()));
		return builder.build();
	}

}
