package com.redis.riot.core;

import org.springframework.batch.item.ItemProcessor;

import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.RedisItemReader;

import io.lettuce.core.codec.RedisCodec;

public abstract class AbstractExport extends AbstractRedisCallable {

	private RedisReaderOptions readerOptions = new RedisReaderOptions();
	private KeyValueProcessorOptions processorOptions = new KeyValueProcessorOptions();

	@Override
	protected <K, V, T> void configure(RedisItemReader<K, V, T> reader) {
		reader.setJobFactory(getJobFactory());
		readerOptions.configure(reader);
		super.configure(reader);
	}

	public RedisReaderOptions getReaderOptions() {
		return readerOptions;
	}

	public void setReaderOptions(RedisReaderOptions options) {
		this.readerOptions = options;
	}

	public KeyValueProcessorOptions getProcessorOptions() {
		return processorOptions;
	}

	public void setProcessorOptions(KeyValueProcessorOptions options) {
		this.processorOptions = options;
	}

	public <K> ItemProcessor<KeyValue<K, Object>, KeyValue<K, Object>> processor(RedisCodec<K, ?> codec) {
		return processorOptions.processor(getEvaluationContext(), codec);
	}

}
