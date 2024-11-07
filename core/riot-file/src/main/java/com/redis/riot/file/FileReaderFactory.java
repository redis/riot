package com.redis.riot.file;

import org.springframework.batch.item.ItemReader;
import org.springframework.core.io.Resource;

public interface FileReaderFactory {

	ItemReader<?> create(Resource resource, FileReaderOptions options);

}
