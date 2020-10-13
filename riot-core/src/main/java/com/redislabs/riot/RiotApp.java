package com.redislabs.riot;

import java.util.List;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import org.springframework.batch.item.redis.support.RedisConnectionBuilder;

import com.redislabs.riot.redis.AbstractRedisCommand;

import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.JdkLoggerFactory;
import lombok.Getter;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.PicocliException;
import picocli.CommandLine.RunFirst;

@Command(usageHelpAutoWidth = true, sortOptions = false, versionProvider = ManifestVersionProvider.class, subcommands = HiddenGenerateCompletion.class, abbreviateSynopsis = true)
public class RiotApp implements Runnable {

	private static final String ROOT_LOGGER = "";

	@Option(names = { "--help" }, usageHelp = true, description = "Show this help message and exit.")
	private boolean helpRequested;
	@Option(names = { "-V", "--version" }, versionHelp = true, description = "Print version information and exit.")
	private boolean versionRequested;
	@Getter
	@Option(names = { "-q", "--quiet" }, description = "Log errors only")
	private boolean quiet;
	@Getter
	@Option(names = { "-d", "--debug" }, description = "Log in debug mode (includes normal stacktrace)")
	private boolean debug;
	@Getter
	@Option(names = { "-i", "--info" }, description = "Set log level to info")
	private boolean info;
	@Getter
	@ArgGroup(heading = "Redis connection options%n", exclusive = false)
	private RedisConnectionOptions redisConnectionOptions = new RedisConnectionOptions();

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public int execute(String... args) {
		try {
			CommandLine commandLine = commandLine();
			ParseResult parsed = commandLine.parseArgs(args);
			InternalLoggerFactory.setDefaultFactory(JdkLoggerFactory.INSTANCE);
			LogManager.getLogManager().reset();
			Logger activeLogger = Logger.getLogger(ROOT_LOGGER);
			ConsoleHandler handler = new ConsoleHandler();
			handler.setLevel(Level.ALL);
			handler.setFormatter(new OneLineLogFormat(isDebug()));
			activeLogger.addHandler(handler);
			Logger.getLogger(ROOT_LOGGER).setLevel(rootLoggingLevel());
			Logger logger = Logger.getLogger("com.redislabs");
			logger.setLevel(packageLoggingLevel());
			ParseResult subcommand = parsed.subcommand();
			if (subcommand != null) {
				Object command = subcommand.commandSpec().userObject();
				if (AbstractImportCommand.class.isAssignableFrom(command.getClass())) {
					AbstractImportCommand<?, ?> importCommand = (AbstractImportCommand<?, ?>) command;
					List<ParseResult> parsedRedisCommands = subcommand.subcommands();
					for (ParseResult parsedRedisCommand : parsedRedisCommands) {
						if (parsedRedisCommand.isUsageHelpRequested()) {
							return commandLine.getExecutionStrategy().execute(parsedRedisCommand);
						}
						importCommand.getRedisCommands()
								.add((AbstractRedisCommand) parsedRedisCommand.commandSpec().userObject());
					}
					commandLine.setExecutionStrategy(new RunFirst());
					return commandLine.getExecutionStrategy().execute(subcommand);
				}
			}
			return commandLine.getExecutionStrategy().execute(parsed);
		} catch (PicocliException e) {
			System.err.println(e.getMessage());
			return 1;
		}
	}

	public CommandLine commandLine() {
		CommandLine commandLine = new CommandLine(this);
		registerConverters(commandLine);
		commandLine.setCaseInsensitiveEnumValuesAllowed(true);
		return commandLine;
	}

	protected void registerConverters(CommandLine commandLine) {
		commandLine.registerConverter(RedisURI.class, new RedisURIConverter());
	}

	@Override
	public void run() {
		CommandLine.usage(this, System.out);
	}

	private Level packageLoggingLevel() {
		if (isQuiet()) {
			return Level.OFF;
		}
		if (isInfo()) {
			return Level.FINE;
		}
		if (isDebug()) {
			return Level.FINEST;
		}
		return Level.INFO;
	}

	private Level rootLoggingLevel() {
		if (isQuiet()) {
			return Level.OFF;
		}
		if (isInfo()) {
			return Level.INFO;
		}
		if (isDebug()) {
			return Level.FINE;
		}
		return Level.SEVERE;
	}

	public StatefulConnection<String, String> connection() {
		RedisConnectionBuilder<?> connectionBuilder = new RedisConnectionBuilder<>();
		redisConnectionOptions.configure(connectionBuilder);
		return connectionBuilder.connection();
	}

	public BaseRedisCommands<String, String> sync(StatefulConnection<String, String> connection) {
		RedisConnectionBuilder<?> connectionBuilder = new RedisConnectionBuilder<>();
		redisConnectionOptions.configure(connectionBuilder);
		return connectionBuilder.sync().apply(connection);
	}

	public BaseRedisAsyncCommands<String, String> async(StatefulConnection<String, String> connection) {
		RedisConnectionBuilder<?> connectionBuilder = new RedisConnectionBuilder<>();
		redisConnectionOptions.configure(connectionBuilder);
		return connectionBuilder.async().apply(connection);
	}

}
