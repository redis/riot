package com.redislabs.riot;

import java.util.Collections;
import java.util.List;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.RedisKeyValueItemReader;
import org.springframework.batch.item.redis.support.KeyValue;

import com.redislabs.riot.processor.KeyValueMapItemProcessor;

import picocli.CommandLine;

public abstract class AbstractExportCommand<O> extends AbstractTransferCommand<KeyValue<String>, O> {

	@CommandLine.Mixin
	private RedisExportOptions options = new RedisExportOptions();
	@CommandLine.Option(names = "--key-regex", defaultValue = "\\w+:(?<id>.+)", description = "Regex for key-field extraction (default: ${DEFAULT-VALUE})", paramLabel = "<str>")
	private String keyRegex;

	@Override
	protected String taskName() {
		return "Exporting from";
	}

	@Override
	protected List<ItemReader<KeyValue<String>>> readers() {
		RedisKeyValueItemReader<String> reader = configure(RedisKeyValueItemReader.builder()
				.scanCount(options.getScanCount()).scanMatch(options.getScanMatch()).batch(options.getBatchSize())
				.queueCapacity(options.getQueueCapacity()).threads(options.getThreads())).build();
		reader.setName(String.valueOf(getRedisURI()));
		return Collections.singletonList(reader);
	}

	protected KeyValueMapItemProcessor keyValueMapItemProcessor() {
		return KeyValueMapItemProcessor.builder().keyRegex(keyRegex).build();
	}

}
