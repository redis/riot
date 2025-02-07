package com.redis.riot;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import com.redis.riot.core.AbstractJobCommand;
import com.redis.riot.core.QuietMapAccessor;
import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.Step;
import com.redis.riot.core.processor.PredicateOperator;
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
public abstract class AbstractImportCommand extends AbstractJobCommand {

	private static final String TASK_NAME = "Importing";
	public static final String VAR_REDIS = "redis";

	@ArgGroup(exclusive = false)
	private RedisWriterArgs targetRedisWriterArgs = new RedisWriterArgs();

	@ArgGroup(exclusive = false)
	private EvaluationContextArgs evaluationContextArgs = new EvaluationContextArgs();

	@ArgGroup(exclusive = false)
	private ImportProcessorArgs processorArgs = new ImportProcessorArgs();

	private RedisContext targetRedisContext;

	/**
	 * Initialized manually during command parsing
	 */
	private List<OperationCommand> importOperationCommands = new ArrayList<>();

	@Override
	protected void initialize() {
		super.initialize();
		targetRedisContext = targetRedisContext();
		targetRedisContext.afterPropertiesSet();
	}

	@Override
	protected void teardown() {
		if (targetRedisContext != null) {
			targetRedisContext.close();
		}
		super.teardown();
	}

	protected List<Operation<String, String, Map<String, Object>, Object>> operations() {
		return importOperationCommands.stream().map(OperationCommand::operation).collect(Collectors.toList());
	}

	protected boolean hasOperations() {
		return !CollectionUtils.isEmpty(importOperationCommands);
	}

	protected Step<Map<String, Object>, Map<String, Object>> step(ItemReader<Map<String, Object>> reader) {
		Assert.isTrue(hasOperations(), "No Redis command specified");
		RedisItemWriter<String, String, Map<String, Object>> writer = operationWriter();
		configureTargetRedisWriter(writer);
		Step<Map<String, Object>, Map<String, Object>> step = new Step<>(reader, writer);
		step.processor(processor());
		step.taskName(TASK_NAME);
		return step;
	}

	protected ItemProcessor<Map<String, Object>, Map<String, Object>> processor() {
		log.info("Creating SpEL evaluation context with {}", evaluationContextArgs);
		StandardEvaluationContext evaluationContext = evaluationContextArgs.evaluationContext();
		evaluationContext.setVariable(VAR_REDIS, targetRedisContext.getConnection().sync());
		evaluationContext.addPropertyAccessor(new QuietMapAccessor());
		return processor(evaluationContext, processorArgs);
	}

	protected abstract RedisContext targetRedisContext();

	public static ItemProcessor<Map<String, Object>, Map<String, Object>> processor(EvaluationContext evaluationContext,
			ImportProcessorArgs args) {
		List<ItemProcessor<Map<String, Object>, Map<String, Object>>> processors = new ArrayList<>();
		if (args.getFilter() != null) {
			processors.add(new FunctionItemProcessor<>(
					new PredicateOperator<>(args.getFilter().predicate(evaluationContext))));
		}
		if (!CollectionUtils.isEmpty(args.getExpressions())) {
			processors.add(new ExpressionProcessor(evaluationContext, args.getExpressions()));
		}
		return RiotUtils.processor(processors);
	}

	protected RedisItemWriter<String, String, Map<String, Object>> operationWriter() {
		return RedisItemWriter.operation(new MultiOperation<>(operations()));
	}

	protected void configureTargetRedisWriter(RedisItemWriter<?, ?, ?> writer) {
		targetRedisContext.configure(writer);
		log.info("Configuring target Redis writer with {}", targetRedisWriterArgs);
		targetRedisWriterArgs.configure(writer);
	}

	public RedisWriterArgs getTargetRedisWriterArgs() {
		return targetRedisWriterArgs;
	}

	public void setTargetRedisWriterArgs(RedisWriterArgs args) {
		this.targetRedisWriterArgs = args;
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
