package com.redis.riot.file;

import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.core.io.WritableResource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.spring.batch.resource.JsonFileItemWriterBuilder;

public class JsonWriterFactory extends AbstractWriterFactory {

	@Override
	public ItemWriter<?> create(WritableResource resource, WriteOptions options) {
		JsonFileItemWriterBuilder<?> writer = new JsonFileItemWriterBuilder<>();
		writer.name(resource.getFilename());
		writer.resource((WritableResource) resource);
		writer.append(options.isAppend());
		writer.encoding(options.getEncoding());
		writer.forceSync(options.isForceSync());
		writer.lineSeparator(options.getLineSeparator());
		writer.saveState(false);
		writer.shouldDeleteIfEmpty(options.isShouldDeleteIfEmpty());
		writer.shouldDeleteIfExists(options.isShouldDeleteIfExists());
		writer.transactional(options.isTransactional());
		writer.jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>(objectMapper(new ObjectMapper())));
		return writer.build();
	}

}
