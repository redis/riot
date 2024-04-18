package com.redis.riot.core;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;

import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.operation.Operation;

import io.lettuce.core.AbstractRedisClient;

public abstract class AbstractImport extends AbstractJobRunnable {

	private RedisWriterOptions writerOptions = new RedisWriterOptions();
	private EvaluationContextOptions evaluationContextOptions = new EvaluationContextOptions();
	private ImportProcessorOptions processorOptions = new ImportProcessorOptions();
	private List<Operation<String, String, Map<String, Object>, Object>> operations;

	@SuppressWarnings("unchecked")
	public void setOperations(Operation<String, String, Map<String, Object>, Object>... operations) {
		setOperations(Arrays.asList(operations));
	}

	public List<Operation<String, String, Map<String, Object>, Object>> getOperations() {
		return operations;
	}

	public void setOperations(List<Operation<String, String, Map<String, Object>, Object>> operations) {
		this.operations = operations;
	}

	public EvaluationContextOptions getEvaluationContextOptions() {
		return evaluationContextOptions;
	}

	public void setEvaluationContextOptions(EvaluationContextOptions evaluationContextOptions) {
		this.evaluationContextOptions = evaluationContextOptions;
	}

	public RedisWriterOptions getWriterOptions() {
		return writerOptions;
	}

	public void setWriterOptions(RedisWriterOptions options) {
		this.writerOptions = options;
	}

	public ImportProcessorOptions getProcessorOptions() {
		return processorOptions;
	}

	public void setProcessorOptions(ImportProcessorOptions options) {
		this.processorOptions = options;
	}

	protected ItemWriter<Map<String, Object>> writer() {
		Assert.notEmpty(operations, "No operation specified");
		return RiotUtils.writer(operations.stream().map(o -> writer(getRedisClient(), o)).collect(Collectors.toList()));
	}

	private <T> ItemWriter<T> writer(AbstractRedisClient client, Operation<String, String, T, Object> operation) {
		RedisItemWriter<String, String, T> writer = RedisItemWriter.operation(operation);
		writer.setClient(client);
		writer(writer, writerOptions);
		return writer;
	}

	protected ItemProcessor<Map<String, Object>, Map<String, Object>> processor() {
		return processorOptions.processor(evaluationContext());
	}

	protected StandardEvaluationContext evaluationContext() {
		return evaluationContextOptions.evaluationContext();
	}

}
