package com.redis.riot.cli;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.expression.Expression;

import com.redis.riot.cli.operation.DelCommand;
import com.redis.riot.cli.operation.ExpireCommand;
import com.redis.riot.cli.operation.GeoaddCommand;
import com.redis.riot.cli.operation.HsetCommand;
import com.redis.riot.cli.operation.JsonSetCommand;
import com.redis.riot.cli.operation.LpushCommand;
import com.redis.riot.cli.operation.OperationCommand;
import com.redis.riot.cli.operation.RpushCommand;
import com.redis.riot.cli.operation.SaddCommand;
import com.redis.riot.cli.operation.SetCommand;
import com.redis.riot.cli.operation.SugaddCommand;
import com.redis.riot.cli.operation.TsAddCommand;
import com.redis.riot.cli.operation.XaddCommand;
import com.redis.riot.cli.operation.ZaddCommand;
import com.redis.riot.core.AbstractMapImport;
import com.redis.riot.core.RiotStep;
import com.redis.spring.batch.writer.WriteOperation;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(subcommands = { ExpireCommand.class, DelCommand.class, GeoaddCommand.class, HsetCommand.class, LpushCommand.class,
        RpushCommand.class, SaddCommand.class, SetCommand.class, XaddCommand.class, ZaddCommand.class, SugaddCommand.class,
        JsonSetCommand.class,
        TsAddCommand.class }, subcommandsRepeatable = true, synopsisSubcommandLabel = "[REDIS COMMAND...]", commandListHeading = "Redis commands:%n")
public abstract class AbstractImportCommand extends AbstractJobCommand {

    @Option(arity = "1..*", names = "--proc", description = "SpEL expressions in the form field1=\"exp\" field2=\"exp\"...", paramLabel = "<f=exp>")
    Map<String, Expression> processorExpressions;

    @Option(names = "--filter", description = "Discard records using a SpEL expression.", paramLabel = "<exp>")
    Expression filter;

    /**
     * Initialized manually during command parsing
     */
    private List<OperationCommand> commands = new ArrayList<>();

    public List<OperationCommand> getCommands() {
        return commands;
    }

    public void setCommands(List<OperationCommand> commands) {
        this.commands = commands;
    }

    protected List<WriteOperation<String, String, Map<String, Object>>> operations() {
        return commands.stream().map(OperationCommand::operation).collect(Collectors.toList());
    }

    @Override
    protected AbstractMapImport getJobExecutable() {
        AbstractMapImport executable = getMapImportExecutable();
        executable.setOperations(operations());
        executable.setProcessorExpressions(processorExpressions);
        executable.setFilterExpression(filter);
        return executable;
    }

    protected abstract AbstractMapImport getMapImportExecutable();

    @Override
    protected String taskName(RiotStep<?, ?> step) {
        return "Importing";
    }

}
