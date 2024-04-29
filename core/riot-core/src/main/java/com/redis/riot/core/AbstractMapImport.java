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
import com.redis.spring.batch.writer.WriteOperation;

public abstract class AbstractMapImport extends AbstractImport {

	private EvaluationContextOptions evaluationContextOptions = new EvaluationContextOptions();
	private ImportProcessorOptions processorOptions = new ImportProcessorOptions();
	private List<WriteOperation<String, String, Map<String, Object>>> operations;

	@SuppressWarnings("unchecked")
	public void setOperations(WriteOperation<String, String, Map<String, Object>>... operations) {
		setOperations(Arrays.asList(operations));
	}

	public List<WriteOperation<String, String, Map<String, Object>>> getOperations() {
		return operations;
	}

	public void setOperations(List<WriteOperation<String, String, Map<String, Object>>> operations) {
		this.operations = operations;
	}

	public EvaluationContextOptions getEvaluationContextOptions() {
		return evaluationContextOptions;
	}

	public void setEvaluationContextOptions(EvaluationContextOptions evaluationContextOptions) {
		this.evaluationContextOptions = evaluationContextOptions;
	}

	public ImportProcessorOptions getProcessorOptions() {
		return processorOptions;
	}

	public void setProcessorOptions(ImportProcessorOptions options) {
		this.processorOptions = options;
	}

	protected ItemWriter<Map<String, Object>> writer() {
		Assert.notEmpty(operations, "No operation specified");
		return RiotUtils.writer(operations.stream().map(this::writer).collect(Collectors.toList()));
	}

	private <T> ItemWriter<T> writer(WriteOperation<String, String, T> operation) {
		RedisItemWriter<String, String, T> writer = RedisItemWriter.operation(operation);
		configure(writer);
		return writer;
	}

	protected ItemProcessor<Map<String, Object>, Map<String, Object>> processor() {
		return processorOptions.processor(evaluationContext());
	}

	protected StandardEvaluationContext evaluationContext() {
		return evaluationContextOptions.evaluationContext();
	}

}
