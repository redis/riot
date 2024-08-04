package com.redis.riot;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import com.redis.riot.core.Expression;
import com.redis.riot.core.QuietMapAccessor;
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
import com.redis.spring.batch.item.redis.common.MultiOperation;
import com.redis.spring.batch.item.redis.common.Operation;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(subcommands = { ExpireCommand.class, DelCommand.class, GeoaddCommand.class, HsetCommand.class,
		LpushCommand.class, RpushCommand.class, SaddCommand.class, SetCommand.class, XaddCommand.class,
		ZaddCommand.class, SugaddCommand.class, JsonSetCommand.class,
		TsAddCommand.class }, subcommandsRepeatable = true, synopsisSubcommandLabel = "[REDIS COMMAND...]", commandListHeading = "Redis commands:%n")
public abstract class AbstractImportCommand<C extends RedisExecutionContext> extends AbstractRedisCommand<C> {

	private static final String TASK_NAME = "Importing";
	private static final String STEP_NAME = "step";
	public static final String VAR_REDIS = "redis";

	@ArgGroup(exclusive = false)
	private RedisWriterArgs redisWriterArgs = new RedisWriterArgs();

	@ArgGroup(exclusive = false)
	private EvaluationContextArgs evaluationContextArgs = new EvaluationContextArgs();

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

	protected Step<Map<String, Object>, Map<String, Object>> step(C context, ItemReader<Map<String, Object>> reader) {
		Assert.isTrue(hasOperations(), "No Redis command specified");
		RedisItemWriter<String, String, Map<String, Object>> writer = operationWriter();
		configure(context, writer);
		Step<Map<String, Object>, Map<String, Object>> step = new Step<>(STEP_NAME, reader, writer);
		step.processor(processor(context));
		step.taskName(TASK_NAME);
		return step;
	}

	protected ItemProcessor<Map<String, Object>, Map<String, Object>> processor(C context) {
		StandardEvaluationContext evaluationContext = evaluationContextArgs.evaluationContext();
		evaluationContext.setVariable(VAR_REDIS, context.getRedisContext().getConnection().sync());
		evaluationContext.addPropertyAccessor(new QuietMapAccessor());
		return processorArgs.processor(evaluationContext);
	}

	protected RedisItemWriter<String, String, Map<String, Object>> operationWriter() {
		return RedisItemWriter.operation(new MultiOperation<>(operations()));
	}

	protected void configure(C context, RedisItemWriter<?, ?, ?> writer) {
		context.configure(writer);
		log.info("Configuring Redis writer with {}", redisWriterArgs);
		redisWriterArgs.configure(writer);
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

	public EvaluationContextArgs getEvaluationContextArgs() {
		return evaluationContextArgs;
	}

	public void setEvaluationContextArgs(EvaluationContextArgs evaluationContextArgs) {
		this.evaluationContextArgs = evaluationContextArgs;
	}

}
