package com.redis.riot.file;

import java.io.IOException;

import org.springframework.batch.item.ItemWriter;
import org.springframework.core.io.WritableResource;

public class FileWriterRegistry extends AbstractRegistry<FileWriterFactory> {

	public ItemWriter<?> writer(String location, FileWriterOptions options) throws IOException {
		WritableResource resource = Files.writableResource(location, options.getFileOptions().getResourceOptions());
		return factory(resource, options.getFileOptions()).create(resource, options);
	}

}
