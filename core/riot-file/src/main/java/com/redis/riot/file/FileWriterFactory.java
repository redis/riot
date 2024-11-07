package com.redis.riot.file;

import org.springframework.batch.item.ItemWriter;
import org.springframework.core.io.WritableResource;

public interface FileWriterFactory {

	ItemWriter<?> create(WritableResource resource, FileWriterOptions options);

}
