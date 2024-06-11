package com.redis.riot;

import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.expression.EvaluationContext;

import com.redis.riot.core.Step;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.common.KeyValue;

import picocli.CommandLine.ArgGroup;

public abstract class AbstractExportCommand extends AbstractRedisArgsCommand {

	private static final String TASK_NAME = "Exporting";
	private static final String STEP_NAME = "export";

	@ArgGroup(exclusive = false)
	private RedisReaderArgs redisReaderArgs = new RedisReaderArgs();

	@ArgGroup(exclusive = false)
	private ExportProcessorArgs processorArgs = new ExportProcessorArgs();

	protected <T> Step<KeyValue<String, Object>, T> step(ItemWriter<T> writer) {
		Step<KeyValue<String, Object>, T> step = new Step<>(STEP_NAME, reader(), writer).taskName(TASK_NAME);
		configureExportStep(step);
		return step;
	}

	private RedisItemReader<String, String, Object> reader() {
		RedisItemReader<String, String, Object> reader = RedisItemReader.struct();
		configure(reader);
		log.info("Configuring Redis reader with {}", redisReaderArgs);
		redisReaderArgs.configure(reader);
		return reader;
	}

	protected ItemProcessor<KeyValue<String, Object>, Map<String, Object>> mapProcessor() {
		return processorArgs.mapProcessor(evaluationContext());
	}

	private EvaluationContext evaluationContext() {
		return evaluationContext(processorArgs);
	}

	protected ItemProcessor<KeyValue<String, Object>, KeyValue<String, Object>> keyValueProcessor() {
		return processorArgs.keyValueProcessor(evaluationContext());
	}

	public RedisReaderArgs getRedisReaderArgs() {
		return redisReaderArgs;
	}

	public void setRedisReaderArgs(RedisReaderArgs args) {
		this.redisReaderArgs = args;
	}

	public ExportProcessorArgs getProcessorArgs() {
		return processorArgs;
	}

	public void setProcessorArgs(ExportProcessorArgs args) {
		this.processorArgs = args;
	}
}
