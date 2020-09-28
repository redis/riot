package com.redislabs.riot;

import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.RedisKeyValueItemReader;
import org.springframework.batch.item.redis.support.KeyValue;

import com.redislabs.riot.processor.KeyValueMapItemProcessor;

import picocli.CommandLine;

public abstract class AbstractExportCommand<O> extends AbstractTransferCommand<KeyValue<String>, O> {

	@CommandLine.Mixin
	private RedisExportOptions options = new RedisExportOptions();
	@CommandLine.Option(names = "--key-regex", description = "Regex for key-field extraction (default: ${DEFAULT-VALUE})", paramLabel = "<str>")
	private String keyRegex = "\\w+:(?<id>.+)";

	protected ItemReader<KeyValue<String>> reader() {
		return configure(RedisKeyValueItemReader.builder().scanCount(options.getScanCount())
				.scanMatch(options.getScanMatch()).batch(options.getBatchSize())
				.queueCapacity(options.getQueueCapacity()).threads(options.getThreads())).build();
	}

	protected ItemProcessor<KeyValue<String>, Map<String, String>> keyValueMapItemProcessor() {
		return KeyValueMapItemProcessor.builder().keyRegex(keyRegex).build();
	}

}
