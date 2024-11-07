package com.redis.riot.file;

import java.io.IOException;

import org.springframework.batch.item.ItemReader;
import org.springframework.core.io.Resource;

public class FileReaderRegistry extends AbstractRegistry<FileReaderFactory> {

	public ItemReader<?> reader(String location, FileReaderOptions options) throws IOException {
		Resource resource = Files.resource(location, options.getFileOptions().getResourceOptions());
		return factory(resource, options.getFileOptions()).create(resource, options);
	}

}
