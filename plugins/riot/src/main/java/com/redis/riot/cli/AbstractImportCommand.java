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
import com.redis.riot.core.MapProcessorOptions;
import com.redis.spring.batch.writer.WriteOperation;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(subcommands = { ExpireCommand.class, DelCommand.class, GeoaddCommand.class, HsetCommand.class,
		LpushCommand.class, RpushCommand.class, SaddCommand.class, SetCommand.class, XaddCommand.class,
		ZaddCommand.class, SugaddCommand.class, JsonSetCommand.class,
		TsAddCommand.class }, subcommandsRepeatable = true, synopsisSubcommandLabel = "[REDIS COMMAND...]", commandListHeading = "Redis commands:%n")
public abstract class AbstractImportCommand extends AbstractRedisCommand {

	@ArgGroup(exclusive = false, heading = "Redis writer options%n")
	private RedisWriterArgs redisWriterArgs = new RedisWriterArgs();

	@Option(arity = "1..*", names = "--proc", description = "SpEL expressions in the form field1=\"exp\" field2=\"exp\"...", paramLabel = "<f=exp>")
	private Map<String, Expression> processorExpressions;

	@Option(names = "--filter", description = "Discard records using a SpEL expression.", paramLabel = "<exp>")
	private Expression filter;

	@ArgGroup(exclusive = false)
	private EvaluationContextArgs evaluationContextArgs = new EvaluationContextArgs();

	/**
	 * Initialized manually during command parsing
	 */
	private List<WriteOperationCommand> commands = new ArrayList<>();

	public List<WriteOperationCommand> getCommands() {
		return commands;
	}

	public void setCommands(List<WriteOperationCommand> commands) {
		this.commands = commands;
	}

	protected List<WriteOperation<String, String, Map<String, Object>>> operations() {
		return commands.stream().map(WriteOperationCommand::operation).collect(Collectors.toList());
	}

	@Override
	protected AbstractImport redisCallable() {
		AbstractImport callable = importCallable();
		callable.setWriterOptions(redisWriterArgs.writerOptions());
		callable.setOperations(operations());
		callable.setEvaluationContextOptions(evaluationContextArgs.evaluationContextOptions());
		callable.setMapProcessorOptions(mapProcessorOptions());
		return callable;
	}

	protected abstract AbstractImport importCallable();

	private MapProcessorOptions mapProcessorOptions() {
		MapProcessorOptions options = new MapProcessorOptions();
		options.setExpressions(processorExpressions);
		options.setFilter(filter);
		return options;
	}

	@Override
	protected String taskName(String stepName) {
		return "Importing";
	}

	public RedisWriterArgs getRedisWriterArgs() {
		return redisWriterArgs;
	}

	public void setRedisWriterArgs(RedisWriterArgs redisWriterArgs) {
		this.redisWriterArgs = redisWriterArgs;
	}

}
