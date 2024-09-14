package com.redis.riot;

import com.redis.riot.core.RiotUtils;
import com.redis.riot.function.StringKeyValue;
import com.redis.riot.function.ToStringKeyValue;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.reader.KeyComparisonItemReader;

import io.lettuce.core.codec.ByteArrayCodec;

import org.springframework.batch.core.Job;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "compare", description = "Compare two Redis databases.")
public class Compare extends AbstractCompareCommand {

	@Option(names = "--stream-msg-id", description = "Compare stream message ids. True by default.", negatable = true, defaultValue = "true", fallbackValue = "true")
	private boolean compareStreamMessageId = DEFAULT_COMPARE_STREAM_MESSAGE_ID;

	@Option(names = "--quick", description = "Skip value comparison.")
	private boolean quick;

	@ArgGroup(exclusive = false)
	private final EvaluationContextArgs evaluationContextArgs = new EvaluationContextArgs();

	@ArgGroup(exclusive = false, heading = "Processor options%n")
	private final KeyValueProcessorArgs processorArgs = new KeyValueProcessorArgs();

	@Override
	protected boolean isQuickCompare() {
		return quick;
	}

	@Override
	protected boolean isIgnoreStreamMessageId() {
		return !compareStreamMessageId;
	}

	@Override
	protected Job job() {
		return job(compareStep());
	}

	private StandardEvaluationContext evaluationContext() {
		log.info("Creating SpEL evaluation context with {}", evaluationContextArgs);
		StandardEvaluationContext evaluationContext = evaluationContextArgs.evaluationContext();
		configure(evaluationContext);
		return evaluationContext;
	}

	private ItemProcessor<KeyValue<byte[], Object>, KeyValue<byte[], Object>> keyValueProcessor() {
		StandardEvaluationContext evaluationContext = evaluationContext();
		log.info("Creating processor with {}", processorArgs);
		ItemProcessor<KeyValue<String, Object>, KeyValue<String, Object>> processor = processorArgs
				.processor(evaluationContext);
		if (processor == null) {
			return null;
		}
		ToStringKeyValue<byte[]> code = new ToStringKeyValue<>(ByteArrayCodec.INSTANCE);
		StringKeyValue<byte[]> decode = new StringKeyValue<>(ByteArrayCodec.INSTANCE);
		return RiotUtils.processor(new FunctionItemProcessor<>(code), processor, new FunctionItemProcessor<>(decode));
	}

	private ItemProcessor<KeyValue<byte[], Object>, KeyValue<byte[], Object>> processor() {
		return RiotUtils.processor(new KeyValueFilter<>(ByteArrayCodec.INSTANCE, log), keyValueProcessor());
	}

	@Override
	protected KeyComparisonItemReader<byte[], byte[]> compareReader() {
		KeyComparisonItemReader<byte[], byte[]> reader = super.compareReader();
		reader.setProcessor(processor());
		return reader;
	}

	public boolean isCompareStreamMessageId() {
		return compareStreamMessageId;
	}

	public void setCompareStreamMessageId(boolean streamMessageId) {
		this.compareStreamMessageId = streamMessageId;
	}

}
