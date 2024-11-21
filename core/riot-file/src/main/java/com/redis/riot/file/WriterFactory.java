package com.redis.riot.file;

import org.springframework.batch.item.ItemWriter;
import org.springframework.core.io.WritableResource;

public interface WriterFactory {

	ItemWriter<?> create(WritableResource resource, WriteOptions options);
}
