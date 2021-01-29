package com.redislabs.riot;

import com.redislabs.riot.redis.AbstractRedisCommand;
import lombok.Getter;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import picocli.CommandLine;
import picocli.CommandLine.*;

import java.util.List;

@Command(usageHelpAutoWidth = true, sortOptions = false, versionProvider = ManifestVersionProvider.class, subcommands = HiddenGenerateCompletion.class, abbreviateSynopsis = true)
public class RiotApp implements Runnable {

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
    @Option(names = {"-S", "--stacktrace"}, description = "Print out the stacktrace for all exceptions.")
    private boolean stacktrace;
    @Getter
    @ArgGroup(heading = "Redis connection options%n", exclusive = false)
    private RedisOptions redisOptions = new RedisOptions();

    public int execute(String... args) {
        try {
            CommandLine commandLine = commandLine();
            ParseResult parseResult = parse(commandLine, args);
            Configurator.setRootLevel(logLevel());
            return commandLine.getExecutionStrategy().execute(parseResult);
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

    private Level logLevel() {
        if (debug) {
            return Level.DEBUG;
        }
        if (info) {
            return Level.INFO;
        }
        if (warn) {
            return Level.WARN;
        }
        if (quiet) {
            return Level.FATAL;
        }
        return Level.ERROR;
    }

}
