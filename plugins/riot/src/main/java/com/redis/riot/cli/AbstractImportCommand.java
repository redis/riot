package com.redis.riot.cli;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
import com.redis.riot.core.StepBuilder;
import com.redis.spring.batch.writer.Operation;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(subcommands = { ExpireCommand.class, DelCommand.class, GeoaddCommand.class, HsetCommand.class, LpushCommand.class,
        RpushCommand.class, SaddCommand.class, SetCommand.class, XaddCommand.class, ZaddCommand.class, SugaddCommand.class,
        JsonSetCommand.class,
        TsAddCommand.class }, subcommandsRepeatable = true, synopsisSubcommandLabel = "[REDIS COMMAND...]", commandListHeading = "Redis commands:%n")
public abstract class AbstractImportCommand extends AbstractJobCommand {

    @ArgGroup(exclusive = false, heading = "Processor options%n")
    ProcessorArgs processorArgs = new ProcessorArgs();

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

    protected List<Operation<String, String, Map<String, Object>>> operations() {
        return commands.stream().map(OperationCommand::operation).collect(Collectors.toList());
    }

    @Override
    protected AbstractMapImport getJobExecutable() {
        AbstractMapImport executable = getMapImportExecutable();
        executable.setOperations(operations());
        executable.setProcessorOptions(processorArgs.processorOptions());
        return executable;
    }

    protected abstract AbstractMapImport getMapImportExecutable();

    @Override
    protected String taskName(StepBuilder<?, ?> step) {
        return "Importing";
    }

    @Override
    protected Supplier<String> extraMessage(StepBuilder<?, ?> step) {
        return null;
    }

}
