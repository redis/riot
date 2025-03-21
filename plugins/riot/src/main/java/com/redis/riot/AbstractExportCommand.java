package com.redis.riot;

import org.springframework.batch.item.ItemWriter;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.redis.riot.core.AbstractJobCommand;
import com.redis.riot.core.Step;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemReader.ReaderMode;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.reader.KeyValueRead;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public abstract class AbstractExportCommand extends AbstractJobCommand {

	public static final ReaderMode DEFAULT_MODE = RedisItemReader.DEFAULT_MODE;

	private static final String TASK_NAME = "Exporting";
	private static final String VAR_SOURCE = "source";

	@Option(names = "--mode", description = "Source for keys: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
	private ReaderMode mode = DEFAULT_MODE;

	@ArgGroup(exclusive = false)
	private RedisReaderArgs readerArgs = new RedisReaderArgs();

	@ArgGroup(exclusive = false)
	private RedisReaderLiveArgs readerLiveArgs = new RedisReaderLiveArgs();

	@ArgGroup(exclusive = false)
	private MemoryUsageArgs readerMemoryUsageArgs = new MemoryUsageArgs();

	private RedisContext sourceRedisContext;

	@Override
	protected void initialize() {
		super.initialize();
		sourceRedisContext = sourceRedisContext();
		sourceRedisContext.afterPropertiesSet();
	}

	@Override
	protected void teardown() {
		if (sourceRedisContext != null) {
			sourceRedisContext.close();
		}
		super.teardown();
	}

	protected void configure(StandardEvaluationContext context) {
		context.setVariable(VAR_SOURCE, sourceRedisContext.getConnection().sync());
	}

	protected void configureSourceRedisReader(RedisItemReader<?, ?> reader) {
		configureAsyncStreamSupport(reader);
		sourceRedisContext.configure(reader);
		log.info("Configuring {} in {} mode", reader.getName(), mode);
		reader.setMode(mode);
		log.info("Configuring {} with {}", reader.getName(), readerArgs);
		readerArgs.configure(reader);
		if (mode != ReaderMode.SCAN) {
			log.info("Configuring {} with {}", reader.getName(), readerLiveArgs);
			readerLiveArgs.configure(reader);
		}
		if (readerMemoryUsageArgs.getLimit() != null && reader.getOperation() instanceof KeyValueRead) {
			log.info("Configuring {} with {}", reader.getName(), readerMemoryUsageArgs);
			readerMemoryUsageArgs.configure(reader);
		}
	}

	protected void configureSourceRedisWriter(RedisItemWriter<?, ?, ?> writer) {
		log.info("Configuring source writer with Redis context");
		sourceRedisContext.configure(writer);
	}

	protected abstract RedisContext sourceRedisContext();

	protected <O> Step<KeyValue<String>, O> step(ItemWriter<O> writer) {
		RedisItemReader<String, String> reader = RedisItemReader.struct();
		configureSourceRedisReader(reader);
		Step<KeyValue<String>, O> step = new ExportStepHelper(log).step(reader, writer);
		step.taskName(TASK_NAME);
		return step;
	}

	public ReaderMode getMode() {
		return mode;
	}

	public void setMode(ReaderMode mode) {
		this.mode = mode;
	}

	public RedisReaderArgs getReaderArgs() {
		return readerArgs;
	}

	public void setReaderArgs(RedisReaderArgs args) {
		this.readerArgs = args;
	}

	public RedisReaderLiveArgs getReaderLiveArgs() {
		return readerLiveArgs;
	}

	public void setReaderLiveArgs(RedisReaderLiveArgs args) {
		this.readerLiveArgs = args;
	}

	public MemoryUsageArgs getReaderMemoryUsageArgs() {
		return readerMemoryUsageArgs;
	}

	public void setReaderMemoryUsageArgs(MemoryUsageArgs args) {
		this.readerMemoryUsageArgs = args;
	}

}
