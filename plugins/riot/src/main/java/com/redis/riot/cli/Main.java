package com.redis.riot.cli;

import java.io.PrintWriter;

import org.slf4j.event.Level;
import org.springframework.expression.Expression;
import org.springframework.util.unit.DataSize;
import org.springframework.util.unit.DataUnit;

import com.redis.riot.cli.operation.OperationCommand;
import com.redis.riot.core.SpelUtils;
import com.redis.riot.core.TemplateExpression;
import com.redis.spring.batch.util.DoubleRange;
import com.redis.spring.batch.util.IntRange;

import picocli.AutoComplete.GenerateCompletion;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.RunFirst;
import picocli.CommandLine.RunLast;

@Command(name = "riot", subcommands = { DatabaseImportCommand.class, DatabaseExportCommand.class, FileDumpImportCommand.class,
        FileImportCommand.class, FileDumpExportCommand.class, FakerImportCommand.class, GeneratorImportCommand.class,
        ReplicationCommand.class, PingCommand.class, GenerateCompletion.class })
public class Main extends BaseCommand implements Runnable {

    private PrintWriter out;

    private PrintWriter err;

    @Option(names = { "-V", "--version" }, versionHelp = true, description = "Print version information and exit.")
    private boolean versionRequested;

    @ArgGroup(exclusive = false, heading = "Redis connection options%n")
    private RedisArgs redisArgs = new RedisArgs();

    public PrintWriter getOut() {
        return out;
    }

    public void setOut(PrintWriter out) {
        this.out = out;
    }

    public PrintWriter getErr() {
        return err;
    }

    public void setErr(PrintWriter err) {
        this.err = err;
    }

    public RedisArgs getRedisArgs() {
        return redisArgs;
    }

    @Override
    public void run() {
        spec.commandLine().usage(out);
    }

    public static void main(String[] args) {
        LoggingArgs.setLogLevel(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, Level.ERROR);
        System.exit(run(args));
    }

    public static int run(String... args) {
        Main cmd = new Main();
        CommandLine commandLine = commandLine(cmd);
        cmd.out = commandLine.getOut();
        cmd.err = commandLine.getErr();
        return execute(commandLine, args);
    }

    public static int run(PrintWriter out, PrintWriter err, String... args) {
        Main cmd = new Main();
        CommandLine commandLine = commandLine(cmd);
        commandLine.setOut(out);
        commandLine.setErr(err);
        cmd.out = out;
        cmd.err = err;
        return execute(commandLine, args);
    }

    private static int execute(CommandLine commandLine, String[] args) {
        return commandLine.execute(args);
    }

    public static CommandLine commandLine(Main cmd) {
        CommandLine commandLine = new CommandLine(cmd);
        commandLine.setExecutionStrategy(Main::executionStrategy);
        commandLine.registerConverter(IntRange.class, Main::intRange);
        commandLine.registerConverter(DoubleRange.class, Main::doubleRange);
        commandLine.registerConverter(DataSize.class, Main::dataSize);
        commandLine.registerConverter(Expression.class, SpelUtils::parse);
        commandLine.registerConverter(TemplateExpression.class, SpelUtils::parseTemplate);
        commandLine.setCaseInsensitiveEnumValuesAllowed(true);
        commandLine.setUnmatchedOptionsAllowedAsOptionParameters(false);
        return commandLine;
    }

    private static int executionStrategy(ParseResult parseResult) {
        for (ParseResult subcommand : parseResult.subcommands()) {
            Object command = subcommand.commandSpec().userObject();
            if (AbstractImportCommand.class.isAssignableFrom(command.getClass())) {
                AbstractImportCommand importCommand = (AbstractImportCommand) command;
                for (ParseResult redisCommand : subcommand.subcommands()) {
                    if (redisCommand.isUsageHelpRequested()) {
                        return new RunLast().execute(redisCommand);
                    }
                    importCommand.getCommands().add((OperationCommand) redisCommand.commandSpec().userObject());
                }
                return new RunFirst().execute(subcommand);
            }
        }
        return new RunLast().execute(parseResult); // default execution strategy
    }

    public static DoubleRange doubleRange(String value) {
        int pos = value.indexOf(DoubleRange.SEPARATOR);
        if (pos >= 0) {
            return DoubleRange.between(parseDouble(value.substring(0, pos), 0),
                    parseDouble(value.substring(pos + DoubleRange.SEPARATOR.length()), Double.MAX_VALUE));
        }
        return DoubleRange.is(parseDouble(value, Double.MAX_VALUE));
    }

    private static double parseDouble(String string, double defaultValue) {
        if (string.isEmpty()) {
            return defaultValue;
        }
        return Double.parseDouble(string);
    }

    public static DataSize dataSize(String value) {
        return DataSize.parse(value, DataUnit.MEGABYTES);
    }

    public static IntRange intRange(String value) {
        int separator;
        if ((separator = value.indexOf(IntRange.SEPARATOR)) >= 0) {
            return IntRange.between(parseInt(value.substring(0, separator), 0),
                    parseInt(value.substring(separator + IntRange.SEPARATOR.length()), Integer.MAX_VALUE));
        }
        return IntRange.is(parseInt(value, Integer.MAX_VALUE));
    }

    private static int parseInt(String string, int defaultValue) {
        if (string.isEmpty()) {
            return defaultValue;
        }
        return Integer.parseInt(string);
    }

}
