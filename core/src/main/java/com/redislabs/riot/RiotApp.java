package com.redislabs.riot;

import com.redislabs.riot.redis.AbstractRedisCommand;
import io.lettuce.core.RedisURI;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.JdkLoggerFactory;
import lombok.Getter;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import picocli.AutoComplete;
import picocli.CommandLine;
import picocli.CommandLine.*;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.logging.*;

@Command(sortOptions = false, versionProvider = RiotApp.ManifestVersionProvider.class, subcommands = RiotApp.HiddenGenerateCompletion.class, abbreviateSynopsis = true)
public class RiotApp extends HelpCommand {

    @Command(hidden = true, name = "generate-completion", usageHelpAutoWidth = true)
    static class HiddenGenerateCompletion extends AutoComplete.GenerateCompletion {
    }

    private static final String ROOT_LOGGER = "";

    @SuppressWarnings("unused")
    @Option(names = {"-V", "--version"}, versionHelp = true, description = "Print version information and exit.")
    private boolean versionRequested;
    @Getter
    @ArgGroup(heading = "Redis connection options%n", exclusive = false)
    private RedisOptions redisOptions = RedisOptions.builder().build();
    @Option(names = {"-q", "--quiet"}, description = "Log errors only.")
    private boolean quiet;
    @Option(names = {"-w", "--warn"}, description = "Set log level to warn.")
    private boolean warning;
    @Option(names = {"-i", "--info"}, description = "Set log level to info.")
    private boolean info;
    @Option(names = {"-d", "--debug"}, description = "Log in debug mode (includes normal stacktrace).")
    private boolean debug;
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
        RiotCommandLine commandLine = new RiotCommandLine(this);
        commandLine.setExecutionStrategy(this::executionStrategy);
        commandLine.setExecutionExceptionHandler(new PrintExceptionMessageHandler());
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

    static class StackTraceOneLineLogFormat extends Formatter {

        private final DateTimeFormatter d = new DateTimeFormatterBuilder().appendValue(ChronoField.HOUR_OF_DAY, 2).appendLiteral(':').appendValue(ChronoField.MINUTE_OF_HOUR, 2).optionalStart().appendLiteral(':').appendValue(ChronoField.SECOND_OF_MINUTE, 2).optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 3, 3, true).toFormatter();
        private final ZoneId offset = ZoneOffset.systemDefault();

        @Override
        public String format(LogRecord record) {
            String message = formatMessage(record);
            ZonedDateTime time = Instant.ofEpochMilli(record.getMillis()).atZone(offset);
            if (record.getThrown() == null) {
                return String.format("%s %s %s\t: %s%n", time.format(d), record.getLevel().getLocalizedName(), record.getLoggerName(), message);
            }
            return String.format("%s %s %s\t: %s%n%s%n", time.format(d), record.getLevel().getLocalizedName(), record.getLoggerName(), message, stackTrace(record));
        }

        private String stackTrace(LogRecord record) {
            StringWriter sw = new StringWriter(4096);
            PrintWriter pw = new PrintWriter(sw);
            record.getThrown().printStackTrace(pw);
            return sw.toString();
        }
    }

    static class OneLineLogFormat extends Formatter {

        @Override
        public String format(LogRecord record) {
            String message = formatMessage(record);
            if (record.getThrown() != null) {
                Throwable rootCause = NestedExceptionUtils.getRootCause(record.getThrown());
                if (rootCause != null && rootCause.getMessage() != null) {
                    return String.format("%s: %s%n", message, rootCause.getMessage());
                }
            }
            return String.format("%s%n", message);
        }

    }

    private static class PrintExceptionMessageHandler implements IExecutionExceptionHandler {

        @Override
        public int handleExecutionException(Exception ex, CommandLine cmd, ParseResult parseResult) {
            // bold red error message
            cmd.getErr().println(cmd.getColorScheme().errorText(ex.getMessage()));
            return cmd.getExitCodeExceptionMapper() != null ? cmd.getExitCodeExceptionMapper().getExitCode(ex) : cmd.getCommandSpec().exitCodeOnExecutionException();
        }

    }

    private class RiotCommandLine extends CommandLine {

        public RiotCommandLine(Object command) {
            super(command);
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public ParseResult parseArgs(String... args) {
            ParseResult parseResult = super.parseArgs(args);
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
                    setExecutionStrategy(RiotApp.this::executionStragegyRunFirst);
                    return subcommand;
                }
            }
            return parseResult;
        }
    }

    static class RedisURIConverter implements CommandLine.ITypeConverter<RedisURI> {

        @Override
        public RedisURI convert(String value) {
            try {
                return RedisURI.create(value);
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid Redis connection string", e);
            }
        }

    }

    static class ExpressionConverter implements CommandLine.ITypeConverter<Expression> {

        private final SpelExpressionParser parser = new SpelExpressionParser();

        @Override
        public Expression convert(String value) throws Exception {
            return parser.parseExpression(value);
        }
    }

    protected void registerConverters(CommandLine commandLine) {
        commandLine.registerConverter(RedisURI.class, new RedisURIConverter());
        commandLine.registerConverter(Expression.class, new ExpressionConverter());
    }

    /**
     * {@link IVersionProvider} implementation that returns version information from
     * the jar file's {@code /META-INF/MANIFEST.MF} file.
     */
    static class ManifestVersionProvider implements IVersionProvider {

        @Override
        public String[] getVersion() {
            return new String[]{
                    // @formatter:off
                "",
                "      ▀        █     @|fg(4;1;1) ██████████████████████████|@",
                " █ ██ █  ███  ████   @|fg(4;2;1) ██████████████████████████|@",
                " ██   █ █   █  █     @|fg(5;4;1) ██████████████████████████|@",
                " █    █ █   █  █     @|fg(1;4;1) ██████████████████████████|@",
                " █    █  ███    ██   @|fg(0;3;4) ██████████████████████████|@"+ "  v" + getVersionString(),
                ""};
                // @formatter:on
        }

        private String getVersionString() {
            try {
                Enumeration<URL> resources = getClass().getClassLoader().getResources("META-INF/MANIFEST.MF");
                while (resources.hasMoreElements()) {
                    URL url = resources.nextElement();
                    Manifest manifest = new Manifest(url.openStream());
                    if (isApplicableManifest(manifest)) {
                        Attributes attr = manifest.getMainAttributes();
                        return String.valueOf(get(attr, "Implementation-Version"));
                    }
                }
            } catch (IOException ex) {
                // ignore
            }
            return "N/A";
        }

        private boolean isApplicableManifest(Manifest manifest) {
            Attributes attributes = manifest.getMainAttributes();
            return "RIOT".equals(get(attributes, "Implementation-Title"));
        }

        private static Object get(Attributes attributes, String key) {
            return attributes.get(new Attributes.Name(key));
        }
    }

}
