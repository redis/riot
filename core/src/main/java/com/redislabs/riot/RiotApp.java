package com.redislabs.riot;

import com.redislabs.riot.redis.AbstractRedisCommand;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.JdkLoggerFactory;
import lombok.Getter;
import picocli.CommandLine;
import picocli.CommandLine.*;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

@Command(usageHelpAutoWidth = true, sortOptions = false, versionProvider = ManifestVersionProvider.class, subcommands = HiddenGenerateCompletion.class, abbreviateSynopsis = true)
public class RiotApp implements Runnable {

    private static final String ROOT_LOGGER = "";

    @Option(names = {"-H", "--help"}, usageHelp = true, description = "Show this help message and exit.")
    private boolean helpRequested;
    @Option(names = {"-V", "--version"}, versionHelp = true, description = "Print version information and exit.")
    private boolean versionRequested;
    @Option(names = {"-q", "--quiet"}, description = "Log errors only.")
    private boolean quiet;
    @Option(names = {"-w", "--warn"}, description = "Set log level to warn.")
    private boolean warn;
    @Option(names = {"-i", "--info"}, description = "Set log level to info.")
    private boolean info;
    @Option(names = {"-d", "--debug"}, description = "Log in debug mode (includes normal stacktrace).")
    private boolean debug;
    @Getter
    @ArgGroup(heading = "Redis connection options%n", exclusive = false)
    private RedisOptions redisOptions = new RedisOptions();

    public int execute(String... args) {
        CommandLine commandLine = commandLine();
        ParseResult[] parseResult = new ParseResult[1];
        try {
            parseResult[0] = parse(commandLine, args);
            initializeLogging();
            return commandLine.getExecutionStrategy().execute(parseResult[0]);
        } catch (ParameterException ex) {
            try {
                return commandLine.getParameterExceptionHandler().handleParseException(ex, args);
            } catch (Exception ex2) {
                return handleUnhandled(ex2, ex.getCommandLine(), ex.getCommandLine().getCommandSpec().exitCodeOnInvalidInput());
            }
        } catch (ExecutionException ex) {
            try {
                Exception cause = ex.getCause() instanceof Exception ? (Exception) ex.getCause() : ex;
                return commandLine.getExecutionExceptionHandler().handleExecutionException(cause, ex.getCommandLine(), parseResult[0]);
            } catch (Exception ex2) {
                return handleUnhandled(ex2, ex.getCommandLine(), ex.getCommandLine().getCommandSpec().exitCodeOnExecutionException());
            }
        } catch (Exception ex) {
            return handleUnhandled(ex, commandLine, commandLine.getCommandSpec().exitCodeOnExecutionException());
        }
    }

    private static String throwableToColorString(Throwable t, Help.ColorScheme existingColorScheme) {
        Help.ColorScheme colorScheme = new Help.ColorScheme.Builder(existingColorScheme).applySystemProperties().build();
        StringWriter stringWriter = new ColoredStackTraceWriter(colorScheme);
        t.printStackTrace(new PrintWriter(stringWriter));
        return stringWriter.toString();
    }

    static class ColoredStackTraceWriter extends StringWriter {
        Help.ColorScheme colorScheme;

        public ColoredStackTraceWriter(Help.ColorScheme colorScheme) { this.colorScheme = colorScheme; }

        @Override
        public void write(String str, int off, int len) {
            List<Help.Ansi.IStyle> styles = str.startsWith("\t") ? colorScheme.stackTraceStyles() : colorScheme.errorStyles();
            super.write(colorScheme.apply(str.substring(off, len), styles).toString());
        }
    }

    private static int handleUnhandled(Exception ex, CommandLine cmd, int defaultExitCode) {
        cmd.getErr().print(throwableToColorString(ex, cmd.getColorScheme()));
        cmd.getErr().flush();
        return mappedExitCode(ex, cmd.getExitCodeExceptionMapper(), defaultExitCode);
    }

    private static int mappedExitCode(Throwable t, IExitCodeExceptionMapper mapper, int defaultExitCode) {
        try {
            return (mapper != null) ? mapper.getExitCode(t) : defaultExitCode;
        } catch (Exception ex) {
            ex.printStackTrace();
            return defaultExitCode;
        }
    }

    private void initializeLogging() {
        InternalLoggerFactory.setDefaultFactory(JdkLoggerFactory.INSTANCE);
        LogManager.getLogManager().reset();
        Logger activeLogger = Logger.getLogger(ROOT_LOGGER);
        ConsoleHandler handler = new ConsoleHandler();
        handler.setLevel(java.util.logging.Level.ALL);
        handler.setFormatter(new OneLineLogFormat());
        activeLogger.addHandler(handler);
        Logger.getLogger(ROOT_LOGGER).setLevel(loggingLevel());
        if (debug) {
            Logger.getLogger("io.lettuce").setLevel(java.util.logging.Level.INFO);
            Logger.getLogger("io.netty").setLevel(java.util.logging.Level.INFO);
        }
    }

    public CommandLine commandLine() {
        CommandLine commandLine = new CommandLine(this);
        commandLine.setExecutionExceptionHandler(new PrintExceptionMessageHandler());
        registerConverters(commandLine);
        commandLine.setCaseInsensitiveEnumValuesAllowed(true);
        return commandLine;
    }

    class PrintExceptionMessageHandler implements IExecutionExceptionHandler {

        @Override
        public int handleExecutionException(Exception ex, CommandLine cmd, ParseResult parseResult) {
            // bold red error message
            cmd.getErr().println(cmd.getColorScheme().errorText(ex.getMessage()));
            return cmd.getExitCodeExceptionMapper() != null ? cmd.getExitCodeExceptionMapper().getExitCode(ex) : cmd.getCommandSpec().exitCodeOnExecutionException();
        }

    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public ParseResult parse(CommandLine commandLine, String[] args) {
        ParseResult parseResult = commandLine.parseArgs(args);
        ParseResult subcommand = parseResult.subcommand();
        if (subcommand != null) {
            Object command = subcommand.commandSpec().userObject();
            if (AbstractImportCommand.class.isAssignableFrom(command.getClass())) {
                AbstractImportCommand<?, ?> importCommand = (AbstractImportCommand<?, ?>) command;
                List<ParseResult> parsedRedisCommands = subcommand.subcommands();
                for (ParseResult parsedRedisCommand : parsedRedisCommands) {
                    if (parsedRedisCommand.isUsageHelpRequested()) {
                        return parsedRedisCommand;
                    }
                    importCommand.getRedisCommands().add((AbstractRedisCommand) parsedRedisCommand.commandSpec().userObject());
                }
                commandLine.setExecutionStrategy(new RunFirst());
                return subcommand;
            }
        }
        return parseResult;
    }

    protected void registerConverters(CommandLine commandLine) {
        commandLine.registerConverter(io.lettuce.core.RedisURI.class, new RedisURIConverter());
    }

    @Override
    public void run() {
        CommandLine.usage(this, System.out);
    }

    private java.util.logging.Level loggingLevel() {
        if (debug) {
            return java.util.logging.Level.FINE;
        }
        if (info) {
            return java.util.logging.Level.INFO;
        }
        if (warn) {
            return java.util.logging.Level.WARNING;
        }
        if (quiet) {
            return java.util.logging.Level.OFF;
        }
        return Level.SEVERE;
    }

}
