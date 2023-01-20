package com.redis.riot;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.awaitility.Awaitility;
import org.codehaus.plexus.util.cli.CommandLineUtils;
import org.springframework.batch.core.JobExecution;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import com.redis.spring.batch.common.DoubleRange;
import com.redis.spring.batch.common.IntRange;

import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisURI;
import io.micrometer.core.instrument.util.IOUtils;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.IExecutionStrategy;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.RunFirst;
import picocli.CommandLine.RunLast;

@Command(versionProvider = ManifestVersionProvider.class, subcommands = GenerateCompletionCommand.class)
public class RiotApp {

	@Mixin
	private HelpOptions helpOptions = new HelpOptions();

	@Option(names = { "-V", "--version" }, versionHelp = true, description = "Print version information and exit.")
	private boolean versionRequested;

	@ArgGroup(heading = "Logging options%n", exclusive = false)
	private LoggingOptions loggingOptions = new LoggingOptions();

	@ArgGroup(heading = "Redis connection options%n", exclusive = false)
	private RedisOptions redisOptions = new RedisOptions();

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

	protected String[] args(String filename) throws Exception {
		try (InputStream inputStream = getClass().getResourceAsStream("/" + filename)) {
			String command = IOUtils.toString(inputStream, Charset.defaultCharset());
			if (command.startsWith("riot-")) {
				command = command.substring(command.indexOf(" ") + 1);
			}

			return CommandLineUtils.translateCommandline(command);
		}
	}

	@SuppressWarnings("unchecked")
	public int execute(String filename, String redisURI, boolean cluster,
			Consumer<CommandLine.ParseResult>... configurators) throws Exception {
		RiotCommandLine commandLine = commandLine();
		CommandLine.ParseResult parseResult = commandLine.parseArgs(args(filename));
		Object command = parseResult.subcommand().commandSpec().commandLine().getCommand();
		if (command instanceof OperationCommand) {
			command = parseResult.subcommand().commandSpec().parent().commandLine().getCommand();
		}
		if (command instanceof AbstractTransferCommand) {
			AbstractTransferCommand transferCommand = (AbstractTransferCommand) command;
			transferCommand.getTransferOptions().setProgressStyle(ProgressStyle.NONE);
		}
		configure(redisURI, cluster);
		for (Consumer<CommandLine.ParseResult> configurator : configurators) {
			configurator.accept(parseResult);
		}
		return commandLine.getExecutionStrategy().execute(parseResult);
	}

	public void configure(String redisURI, boolean cluster) {
		loggingOptions.setInfo(true);
		loggingOptions.setStacktrace(true);
		redisOptions.setUri(RedisURI.create(redisURI));
		redisOptions.setCluster(cluster);
	}

	protected void awaitTermination(JobExecution execution) {
		Awaitility.await().timeout(Duration.ofSeconds(60)).until(() -> !execution.isRunning());
	}

}
