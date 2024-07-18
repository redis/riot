package com.redis.riot;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.expression.EvaluationContext;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import com.redis.riot.core.Expression;
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
	private static final String STEP_NAME = "import";

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
		Assert.isTrue(hasOperations(), "No Redis command specified");
		return new Step<>(STEP_NAME, reader, mapWriter()).processor(mapProcessor()).taskName(TASK_NAME);
	}

	protected ItemProcessor<Map<String, Object>, Map<String, Object>> mapProcessor() {
		return processorArgs.processor(connection);
	}

	protected ItemWriter<Map<String, Object>> mapWriter() {
		return RiotUtils.writer(operations().stream().map(RedisItemWriter::operation).map(this::configure));
	}

	protected <T extends RedisItemWriter<?, ?, ?>> T configure(T writer) {
		writer.setClient(client.getClient());
		log.info("Configuring Redis writer with {}", redisWriterArgs);
		redisWriterArgs.configure(writer);
		return writer;
	}

	static class ExpressionProcessor implements ItemProcessor<Map<String, Object>, Map<String, Object>> {

		private final EvaluationContext context;
		private final Map<String, Expression> expressions;

		public ExpressionProcessor(EvaluationContext context, Map<String, Expression> expressions) {
			this.context = context;
			this.expressions = expressions;
		}

		@Override
		public Map<String, Object> process(Map<String, Object> item) throws Exception {
			expressions.forEach((k, v) -> item.put(k, v.getValue(context, item)));
			return item;
		}

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
