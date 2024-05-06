package com.redis.riot.core;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.writer.WriteOperation;

public abstract class AbstractImport extends AbstractRedisCallable {

	private RedisWriterOptions writerOptions = new RedisWriterOptions();
	private List<WriteOperation<String, String, Map<String, Object>>> operations;
	private MapProcessorOptions mapProcessorOptions = new MapProcessorOptions();

	protected ItemProcessor<Map<String, Object>, Map<String, Object>> mapProcessor() {
		return mapProcessorOptions.processor(evaluationContext);
	}

	protected boolean hasOperations() {
		return !CollectionUtils.isEmpty(operations);
	}

	protected void assertHasOperations() {
		Assert.isTrue(hasOperations(), "No Redis command specified");
	}

	protected ItemWriter<Map<String, Object>> mapWriter() {
		assertHasOperations();
		return RiotUtils.writer(operations.stream().map(this::writer).collect(Collectors.toList()));
	}

	public List<WriteOperation<String, String, Map<String, Object>>> getOperations() {
		return operations;
	}

	@SuppressWarnings("unchecked")
	public void setOperations(WriteOperation<String, String, Map<String, Object>>... operations) {
		setOperations(Arrays.asList(operations));
	}

	public void setOperations(List<WriteOperation<String, String, Map<String, Object>>> operations) {
		this.operations = operations;
	}

	public MapProcessorOptions getMapProcessorOptions() {
		return mapProcessorOptions;
	}

	public void setMapProcessorOptions(MapProcessorOptions mapProcessorOptions) {
		this.mapProcessorOptions = mapProcessorOptions;
	}

	public RedisWriterOptions getWriterOptions() {
		return writerOptions;
	}

	public void setWriterOptions(RedisWriterOptions options) {
		this.writerOptions = options;
	}

	@Override
	protected <K, V, T> void configure(RedisItemWriter<K, V, T> writer) {
		writerOptions.configure(writer);
		super.configure(writer);
	}

	protected <T> ItemWriter<T> writer(WriteOperation<String, String, T> operation) {
		RedisItemWriter<String, String, T> writer = RedisItemWriter.operation(operation);
		configure(writer);
		return writer;
	}

}
