package com.redis.riot.cli;

import java.io.PrintWriter;

import org.springframework.expression.Expression;
import org.springframework.util.unit.DataSize;

import com.redis.riot.core.TemplateExpression;
import com.redis.riot.core.operation.OperationBuilder;
import com.redis.spring.batch.util.DoubleRange;
import com.redis.spring.batch.util.IntRange;

import picocli.AutoComplete.GenerateCompletion;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.IExecutionStrategy;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.RunFirst;
import picocli.CommandLine.RunLast;

@Command(name = "riot", subcommands = { DatabaseImportCommand.class, DatabaseExportCommand.class, FileDumpImportCommand.class,
        FileImportCommand.class, FileDumpExportCommand.class, FakerImportCommand.class, GeneratorImportCommand.class,
        ReplicationCommand.class, Ping.class, GenerateCompletion.class })
public class Main extends BaseCommand implements Runnable, IO {

    private PrintWriter out;

    private PrintWriter err;

    @Option(names = { "-V", "--version" }, versionHelp = true, description = "Print version information and exit.")
    private boolean versionRequested;

    @ArgGroup(exclusive = false, heading = "Redis connection options%n")
    private RedisArgs redisArgs = new RedisArgs();

    @Override
    public PrintWriter getOut() {
        return out;
    }

    @Override
    public void setOut(PrintWriter out) {
        this.out = out;
    }

    @Override
    public PrintWriter getErr() {
        return err;
    }

    @Override
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
        commandLine.setExecutionStrategy(new ExecutionStrategy());
        commandLine.registerConverter(IntRange.class, new IntRangeTypeConverter());
        commandLine.registerConverter(DoubleRange.class, new DoubleRangeTypeConverter());
        commandLine.registerConverter(DataSize.class, new DataSizeTypeConverter());
        commandLine.registerConverter(Expression.class, new ExpressionTypeConverter());
        commandLine.registerConverter(TemplateExpression.class, new TemplateExpressionTypeConverter());
        commandLine.setCaseInsensitiveEnumValuesAllowed(true);
        commandLine.setUnmatchedOptionsAllowedAsOptionParameters(false);
        return commandLine;
    }

    private static class ExecutionStrategy implements IExecutionStrategy {

        @Override
        public int execute(ParseResult parseResult) {
            for (ParseResult subcommand : parseResult.subcommands()) {
                Object command = subcommand.commandSpec().userObject();
                if (AbstractImportCommand.class.isAssignableFrom(command.getClass())) {
                    AbstractImportCommand importCommand = (AbstractImportCommand) command;
                    for (ParseResult redisCommand : subcommand.subcommands()) {
                        if (redisCommand.isUsageHelpRequested()) {
                            return new RunLast().execute(redisCommand);
                        }
                        importCommand.getCommands().add((OperationBuilder) redisCommand.commandSpec().userObject());
                    }
                    return new RunFirst().execute(subcommand);
                }
            }
            return new RunLast().execute(parseResult); // default execution strategy
        }

    }

}
