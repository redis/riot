package com.redislabs.riot;

import io.lettuce.core.RedisURI;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.JdkLoggerFactory;
import lombok.Getter;
import lombok.Setter;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParseResult;

import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

@Command(sortOptions = false, versionProvider = ManifestVersionProvider.class, subcommands = GenerateCompletionCommand.class, abbreviateSynopsis = true)
public class RiotApp extends HelpCommand {

    private static final String ROOT_LOGGER = "";

    @SuppressWarnings("unused")
    @Option(names = {"-V", "--version"}, versionHelp = true, description = "Print version information and exit.")
    private boolean versionRequested;
    @Getter
    @ArgGroup(heading = "Redis connection options%n", exclusive = false)
    private RedisOptions redisOptions = RedisOptions.builder().build();
    @Setter
    @Option(names = {"-q", "--quiet"}, description = "Log errors only.")
    private boolean quiet;
    @Setter
    @Option(names = {"-w", "--warn"}, description = "Set log level to warn.")
    private boolean warning;
    @Setter
    @Option(names = {"-i", "--info"}, description = "Set log level to info.")
    private boolean info;
    @Setter
    @Option(names = {"-d", "--debug"}, description = "Log in debug mode (includes normal stacktrace).")
    private boolean debug;
    @Setter
    @Option(names = "--stacktrace", description = "Print out the stacktrace for all exceptions..")
    private boolean stacktrace;

    private int executionStrategy(ParseResult parseResult) {
        configureLogging();
        return new CommandLine.RunLast().execute(parseResult); // default execution strategy
    }

    private int executionStragegyRunFirst(ParseResult parseResult) {
        configureLogging();
        return new CommandLine.RunFirst().execute(parseResult);
    }

    private void configureLogging() {
        Level level = logLevel();
        InternalLoggerFactory.setDefaultFactory(JdkLoggerFactory.INSTANCE);
        LogManager.getLogManager().reset();
        Logger activeLogger = Logger.getLogger(ROOT_LOGGER);
        ConsoleHandler handler = new ConsoleHandler();
        handler.setLevel(Level.ALL);
        handler.setFormatter(debug || stacktrace ? new StackTraceOneLineLogFormat() : new OneLineLogFormat());
        activeLogger.addHandler(handler);
        Logger.getLogger(ROOT_LOGGER).setLevel(level);
    }

    public int execute(String... args) {
        return commandLine().execute(args);
    }

    public RiotCommandLine commandLine() {
        RiotCommandLine commandLine = new RiotCommandLine(this, this::executionStragegyRunFirst);
        commandLine.setExecutionStrategy(this::executionStrategy);
        commandLine.setExecutionExceptionHandler(this::handleExecutionException);
        registerConverters(commandLine);
        commandLine.setCaseInsensitiveEnumValuesAllowed(true);
        return commandLine;
    }

    private java.util.logging.Level logLevel() {
        if (debug) {
            return java.util.logging.Level.FINE;
        }
        if (info) {
            return java.util.logging.Level.INFO;
        }
        if (warning) {
            return java.util.logging.Level.WARNING;
        }
        if (quiet) {
            return java.util.logging.Level.OFF;
        }
        return Level.SEVERE;
    }

    private int handleExecutionException(Exception ex, CommandLine cmd, ParseResult parseResult) {
        // bold red error message
        cmd.getErr().println(cmd.getColorScheme().errorText(ex.getMessage()));
        return cmd.getExitCodeExceptionMapper() != null ? cmd.getExitCodeExceptionMapper().getExitCode(ex) : cmd.getCommandSpec().exitCodeOnExecutionException();
    }

    protected void registerConverters(CommandLine commandLine) {
        commandLine.registerConverter(RedisURI.class, RedisURI::create);
        SpelExpressionParser parser = new SpelExpressionParser();
        commandLine.registerConverter(Expression.class, parser::parseExpression);
    }

}
