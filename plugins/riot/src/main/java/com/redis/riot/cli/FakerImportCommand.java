package com.redis.riot.cli;

import com.redis.riot.core.StepBuilder;
import com.redis.riot.faker.FakerImport;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "faker-import", description = "Import from Faker.")
public class FakerImportCommand extends AbstractImportCommand {

    @ArgGroup(exclusive = false)
    FakerImportArgs fakerImportArgs = new FakerImportArgs();

    @Override
    protected FakerImport getMapImportExecutable() {
        FakerImport executable = new FakerImport(redisClient());
        executable.setFields(fakerImportArgs.fields);
        executable.setCount(fakerImportArgs.count);
        executable.setLocale(fakerImportArgs.locale);
        executable.setSearchIndex(fakerImportArgs.searchIndex);
        return executable;
    }

    @Override
    protected long size(StepBuilder<?, ?> step) {
        return fakerImportArgs.count;
    }

}
