package com.redislabs.riot.file;

import java.io.IOException;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.core.io.WritableResource;

import picocli.CommandLine.Command;

@Command(name = "export-redis", description = "Export to a file")
public class RedisFileExportCommand extends AbstractFileExportCommand<KeyValue<String>> {

	@Override
	protected ItemProcessor<KeyValue<String>, KeyValue<String>> processor() {
		return null;
	}

	@SuppressWarnings("incomplete-switch")
	@Override
	protected ItemWriter<KeyValue<String>> writer(FileType fileType, WritableResource resource) throws IOException {
		switch (fileType) {
		case JSON:
			return jsonWriter(resource);
		case XML:
			return xmlWriter(resource);
		}
		throw new IllegalArgumentException("Unsupported file type: " + fileType);
	}

}
