package com.redis.riot.cli;

import com.redis.riot.core.RiotStep;
import com.redis.riot.file.FileDumpImport;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "dump-import", description = "Import Redis data files into Redis.")
public class FileDumpImportCommand extends AbstractStructImportCommand {

    @ArgGroup(exclusive = false)
    FileDumpImportArgs args = new FileDumpImportArgs();

    @Override
    protected FileDumpImport importExecutable() {
        FileDumpImport executable = new FileDumpImport();
        executable.setFiles(args.files);
        executable.setFileOptions(args.fileOptions());
        executable.setType(args.type);
        return executable;
    }

    @Override
    protected String taskName(RiotStep<?, ?> step) {
        return "Importing";
    }

}
