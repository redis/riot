package com.redislabs.riot;

import java.util.Collections;
import java.util.List;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.RedisKeyValueItemReader;
import org.springframework.batch.item.redis.support.KeyValue;

import com.redislabs.riot.processor.KeyValueMapItemProcessor;

import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

public abstract class AbstractExportCommand<O> extends AbstractTransferCommand<KeyValue<String>, O> {

	@Mixin
	private RedisExportOptions options = new RedisExportOptions();
	@Option(names = "--key-regex", description = "Regex for key-field extraction (default: ${DEFAULT-VALUE})", paramLabel = "<str>")
	private String keyRegex = "\\w+:(?<id>.+)";

	@Override
	protected String taskName() {
		return "Exporting from";
	}

	@Override
	protected List<ItemReader<KeyValue<String>>> readers() {
		RedisKeyValueItemReader<String> reader = configure(RedisKeyValueItemReader.builder()
				.scanCount(options.getScanCount()).scanMatch(options.getScanMatch()).batch(options.getReaderBatchSize())
				.queueCapacity(options.getQueueCapacity()).threads(options.getReaderThreads())).build();
		reader.setName(String.valueOf(getRedisURI()));
		return Collections.singletonList(reader);
	}

	protected KeyValueMapItemProcessor keyValueMapItemProcessor() {
		return KeyValueMapItemProcessor.builder().keyRegex(keyRegex).build();
	}

}
