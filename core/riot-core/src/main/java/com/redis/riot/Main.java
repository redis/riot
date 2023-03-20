package com.redis.riot;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import com.redis.riot.db.DatabaseExportCommand;
import com.redis.riot.db.DatabaseImportCommand;
import com.redis.riot.file.FileDumpImportCommand;
import com.redis.riot.file.FileExportCommand;
import com.redis.riot.file.FileImportCommand;
import com.redis.riot.gen.FakerHelpCommand;
import com.redis.riot.gen.FakerImportCommand;
import com.redis.riot.gen.GenerateCommand;
import com.redis.riot.redis.InfoCommand;
import com.redis.riot.redis.LatencyCommand;
import com.redis.riot.redis.PingCommand;
import com.redis.riot.replicate.CompareCommand;
import com.redis.riot.replicate.ReplicateCommand;
import com.redis.spring.batch.common.DoubleRange;
import com.redis.spring.batch.common.IntRange;

import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisURI;
import picocli.AutoComplete.GenerateCompletion;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.IExecutionStrategy;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.RunFirst;
import picocli.CommandLine.RunLast;

@Command(name = "riot", usageHelpAutoWidth = true, versionProvider = ManifestVersionProvider.class, subcommands = {
		DatabaseImportCommand.class, DatabaseExportCommand.class, FileImportCommand.class, FileExportCommand.class,
		FileDumpImportCommand.class, ReplicateCommand.class, CompareCommand.class, GenerateCommand.class,
		FakerImportCommand.class, FakerHelpCommand.class, InfoCommand.class, LatencyCommand.class, PingCommand.class,
		GenerateCompletion.class })
public class Main {

	@Mixin
	private HelpOptions helpOptions = new HelpOptions();

	@Option(names = { "-V", "--version" }, versionHelp = true, description = "Print version information and exit.")
	private boolean versionRequested;

	@ArgGroup(heading = "Logging options%n", exclusive = false)
	private LoggingOptions loggingOptions = new LoggingOptions();

	@ArgGroup(heading = "Redis connection options%n", exclusive = false)
	private RedisOptions redisOptions = new RedisOptions();

	public static void main(String[] args) {
		System.exit(new Main().execute(args));
	}

	public RedisOptions getRedisOptions() {
		return redisOptions;
	}

	public LoggingOptions getLoggingOptions() {
		return loggingOptions;
	}

	protected int execute(IExecutionStrategy strategy, ParseResult parseResult) {
		configureLogging();
		Logger log = Logger.getLogger(getClass().getName());
		log.log(Level.FINE, "Running {0} {1} with {2}", new Object[] { parseResult.commandSpec().name(),
				ManifestVersionProvider.getVersionString(), parseResult.originalArgs() });
		return strategy.execute(parseResult);
	}

	protected void configureLogging() {
		loggingOptions.configure();
		Logger.getLogger("com.amazonaws").setLevel(Level.SEVERE);
	}

	public int execute(String... args) {
		return commandLine().execute(args);
	}

	public RiotCommandLine commandLine() {
		RiotCommandLine commandLine = new RiotCommandLine(this, r -> execute(new RunFirst(), r));
		commandLine.setExecutionStrategy(r -> execute(new RunLast(), r));
		commandLine.setExecutionExceptionHandler(this::handleExecutionException);
		registerConverters(commandLine);
		commandLine.setCaseInsensitiveEnumValuesAllowed(true);
		commandLine.setUnmatchedOptionsAllowedAsOptionParameters(false);
		return commandLine;
	}

	private int handleExecutionException(Exception ex, CommandLine cmd, ParseResult parseResult) {
		// bold red error message
		cmd.getErr().println(cmd.getColorScheme().errorText(ex.getMessage()));
		return cmd.getExitCodeExceptionMapper() != null ? cmd.getExitCodeExceptionMapper().getExitCode(ex)
				: cmd.getCommandSpec().exitCodeOnExecutionException();
	}

	protected void registerConverters(CommandLine commandLine) {
		commandLine.registerConverter(RedisURI.class, RedisURI::create);
		commandLine.registerConverter(IntRange.class, new IntRangeTypeConverter());
		commandLine.registerConverter(DoubleRange.class, new DoubleRangeTypeConverter());
		SpelExpressionParser parser = new SpelExpressionParser();
		commandLine.registerConverter(Expression.class, parser::parseExpression);
		commandLine.registerConverter(ReadFrom.class, ReadFrom::valueOf);
	}

}
