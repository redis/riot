package com.redis.riot;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import com.redis.riot.core.QuietMapAccessor;
import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.Step;
import com.redis.riot.operation.DelCommand;
import com.redis.riot.operation.ExpireCommand;
import com.redis.riot.operation.GeoaddCommand;
import com.redis.riot.operation.HsetCommand;
import com.redis.riot.operation.JsonSetCommand;
import com.redis.riot.operation.LpushCommand;
import com.redis.riot.operation.OperationCommand;
import com.redis.riot.operation.RpushCommand;
import com.redis.riot.operation.SaddCommand;
import com.redis.riot.operation.SetCommand;
import com.redis.riot.operation.SugaddCommand;
import com.redis.riot.operation.TsAddCommand;
import com.redis.riot.operation.XaddCommand;
import com.redis.riot.operation.ZaddCommand;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.Operation;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(subcommands = { ExpireCommand.class, DelCommand.class, GeoaddCommand.class, HsetCommand.class,
		LpushCommand.class, RpushCommand.class, SaddCommand.class, SetCommand.class, XaddCommand.class,
		ZaddCommand.class, SugaddCommand.class, JsonSetCommand.class,
		TsAddCommand.class }, subcommandsRepeatable = true, synopsisSubcommandLabel = "[REDIS COMMAND...]", commandListHeading = "Redis commands:%n")
public abstract class AbstractImportCommand extends AbstractRedisArgsCommand {

	private static final String TASK_NAME = "Importing";

	@ArgGroup(exclusive = false)
	private RedisWriterArgs redisWriterArgs = new RedisWriterArgs();

	@ArgGroup(exclusive = false)
	private ImportProcessorArgs processorArgs = new ImportProcessorArgs();

	/**
	 * Initialized manually during command parsing
	 */
	private List<OperationCommand> importOperationCommands = new ArrayList<>();

	protected List<Operation<String, String, Map<String, Object>, Object>> operations() {
		return importOperationCommands.stream().map(OperationCommand::operation).collect(Collectors.toList());
	}

	protected boolean hasOperations() {
		return !CollectionUtils.isEmpty(importOperationCommands);
	}

	protected Step<Map<String, Object>, Map<String, Object>> step(ItemReader<Map<String, Object>> reader) {
		return new Step<>(reader, mapWriter()).taskName(TASK_NAME);
	}

	protected ItemWriter<Map<String, Object>> mapWriter() {
		Assert.isTrue(hasOperations(), "No Redis command specified");
		return RiotUtils.writer(operations().stream().map(this::writer).collect(Collectors.toList()));
	}

	protected void configure(RedisItemWriter<?, ?, ?> writer) {
		writer.setClient(client.getClient());
		log.info("Configuring Redis writer with {}", redisWriterArgs);
		redisWriterArgs.configure(writer);
	}

	protected <T> RedisItemWriter<String, String, T> writer(Operation<String, String, T, Object> operation) {
		RedisItemWriter<String, String, T> writer = RedisItemWriter.operation(operation);
		configure(writer);
		return writer;
	}

	@SuppressWarnings("rawtypes")
	protected ItemProcessor processor() {
		if (hasOperations()) {
			return mapProcessor();
		}
		return processorArgs.getKeyValueProcessorArgs().processor(evaluationContext());
	}

	private StandardEvaluationContext evaluationContext() {
		return evaluationContext(processorArgs.getEvaluationContextArgs());
	}

	protected ItemProcessor<Map<String, Object>, Map<String, Object>> mapProcessor() {
		log.info("Creating map processor with {}", processorArgs);
		StandardEvaluationContext evaluationContext = evaluationContext(processorArgs.getEvaluationContextArgs());
		evaluationContext.addPropertyAccessor(new QuietMapAccessor());
		return processorArgs.mapProcessor(evaluationContext);
	}

	public RedisWriterArgs getRedisWriterArgs() {
		return redisWriterArgs;
	}

	public void setRedisWriterArgs(RedisWriterArgs redisWriterArgs) {
		this.redisWriterArgs = redisWriterArgs;
	}

	public List<OperationCommand> getImportOperationCommands() {
		return importOperationCommands;
	}

	public void setImportOperationCommands(OperationCommand... commands) {
		setImportOperationCommands(Arrays.asList(commands));
	}

	public void setImportOperationCommands(List<OperationCommand> commands) {
		this.importOperationCommands = commands;
	}

	public ImportProcessorArgs getProcessorArgs() {
		return processorArgs;
	}

	public void setProcessorArgs(ImportProcessorArgs args) {
		this.processorArgs = args;
	}

}
