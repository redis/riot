package com.redis.riot.cli;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.expression.Expression;

import com.redis.riot.cli.redis.DelCommand;
import com.redis.riot.cli.redis.ExpireCommand;
import com.redis.riot.cli.redis.GeoaddCommand;
import com.redis.riot.cli.redis.HsetCommand;
import com.redis.riot.cli.redis.JsonSetCommand;
import com.redis.riot.cli.redis.LpushCommand;
import com.redis.riot.cli.redis.RpushCommand;
import com.redis.riot.cli.redis.SaddCommand;
import com.redis.riot.cli.redis.SetCommand;
import com.redis.riot.cli.redis.SugaddCommand;
import com.redis.riot.cli.redis.TsAddCommand;
import com.redis.riot.cli.redis.XaddCommand;
import com.redis.riot.cli.redis.ZaddCommand;
import com.redis.riot.core.AbstractImport;
import com.redis.riot.core.ImportProcessorOptions;
import com.redis.riot.core.RiotStep;
import com.redis.spring.batch.common.Operation;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(subcommands = { ExpireCommand.class, DelCommand.class, GeoaddCommand.class, HsetCommand.class,
		LpushCommand.class, RpushCommand.class, SaddCommand.class, SetCommand.class, XaddCommand.class,
		ZaddCommand.class, SugaddCommand.class, JsonSetCommand.class,
		TsAddCommand.class }, subcommandsRepeatable = true, synopsisSubcommandLabel = "[REDIS COMMAND...]", commandListHeading = "Redis commands:%n")
public abstract class AbstractImportCommand extends AbstractJobCommand {

	@Option(arity = "1..*", names = "--proc", description = "SpEL expressions in the form field1=\"exp\" field2=\"exp\"...", paramLabel = "<f=exp>")
	Map<String, Expression> processorExpressions;

	@Option(names = "--filter", description = "Discard records using a SpEL expression.", paramLabel = "<exp>")
	Expression filter;

	@ArgGroup(exclusive = false)
	EvaluationContextArgs evaluationContextArgs = new EvaluationContextArgs();

	/**
	 * Initialized manually during command parsing
	 */
	private List<RedisCommand> commands = new ArrayList<>();

	public List<RedisCommand> getCommands() {
		return commands;
	}

	public void setCommands(List<RedisCommand> commands) {
		this.commands = commands;
	}

	protected List<Operation<String, String, Map<String, Object>, Object>> operations() {
		return commands.stream().map(RedisCommand::operation).collect(Collectors.toList());
	}

	@Override
	protected AbstractImport jobRunnable() {
		AbstractImport runnable = importRunnable();
		runnable.setOperations(operations());
		runnable.setEvaluationContextOptions(evaluationContextArgs.evaluationContextOptions());
		runnable.setProcessorOptions(processorOptions());
		return runnable;
	}

	private ImportProcessorOptions processorOptions() {
		ImportProcessorOptions options = new ImportProcessorOptions();
		options.setProcessorExpressions(processorExpressions);
		options.setFilterExpression(filter);
		return options;
	}

	protected abstract AbstractImport importRunnable();

	@Override
	protected String taskName(RiotStep<?, ?> step) {
		return "Importing";
	}

}
