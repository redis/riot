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

public abstract class AbstractMapImport extends AbstractImport {

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

	private <T> ItemWriter<T> writer(Operation<String, String, T, Object> operation) {
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
