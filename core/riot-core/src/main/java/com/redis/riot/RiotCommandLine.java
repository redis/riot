package com.redis.riot;

import com.redis.riot.redis.AbstractRedisCommand;
import picocli.CommandLine;

import java.util.List;

public class RiotCommandLine extends CommandLine {

    private final RiotApp app;
    private final IExecutionStrategy executionStrategy;

    public RiotCommandLine(RiotApp app, IExecutionStrategy executionStrategy) {
        super(app);
        this.app = app;
        this.executionStrategy = executionStrategy;
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
                setExecutionStrategy(executionStrategy);
                return subcommand;
            }
        }
        return parseResult;
    }
}