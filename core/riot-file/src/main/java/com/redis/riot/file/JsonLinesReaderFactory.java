package com.redis.riot.file;

import java.util.Map;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.JsonLineMapper;
import org.springframework.core.io.Resource;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonLinesReaderFactory extends AbstractReaderFactory {

	@Override
	public ItemReader<?> create(Resource resource, ReadOptions options) {
		if (Map.class.isAssignableFrom(options.getItemType())) {
			FlatFileItemReaderBuilder<Map<String, Object>> reader = flatFileReader(options);
			reader.resource(resource);
			reader.lineMapper(new JsonLineMapper());
			reader.fieldSetMapper(new MapFieldSetMapper());
			return reader.build();
		}
		FlatFileItemReaderBuilder<Object> reader = flatFileReader(options);
		reader.resource(resource);
		ObjectMapper objectMapper = objectMapper(new ObjectMapper(), options);
		reader.lineMapper(new ObjectMapperLineMapper<>(objectMapper, options.getItemType()));
		return reader.build();
	}

}
