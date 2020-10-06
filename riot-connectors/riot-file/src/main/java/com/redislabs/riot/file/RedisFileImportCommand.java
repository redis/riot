package com.redislabs.riot.file;

import java.io.IOException;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.redis.RedisKeyValueItemWriter;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.batch.item.xml.XmlItemReader;
import org.springframework.core.io.Resource;

import picocli.CommandLine.Command;

@Command(name = "import-redis", description = "Import Redis data file(s)")
public class RedisFileImportCommand extends AbstractFileImportCommand<KeyValue<String>, KeyValue<String>> {

	@SuppressWarnings({ "incomplete-switch", "rawtypes", "unchecked" })
	protected AbstractItemStreamItemReader<KeyValue<String>> reader(String file, FileType fileType, Resource resource)
			throws IOException {
		switch (fileType) {
		case JSON:
			return (JsonItemReader) jsonReader(resource, KeyValue.class);
		case XML:
			return (XmlItemReader) xmlReader(resource, KeyValue.class);
		}
		throw new IllegalArgumentException("Unsupported file type: " + fileType);
	}

	@Override
	protected ItemProcessor<KeyValue<String>, KeyValue<String>> processor() {
		return new JsonKeyValueItemProcessor();
	}

	@Override
	protected ItemWriter<KeyValue<String>> writer() throws Exception {
		return getApp().configure(RedisKeyValueItemWriter.builder()).build();
	}

}
