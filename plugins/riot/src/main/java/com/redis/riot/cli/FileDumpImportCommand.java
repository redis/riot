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
    protected FileDumpImport importRunnable() {
        FileDumpImport runnable = new FileDumpImport();
        runnable.setFiles(args.files);
        runnable.setFileOptions(args.fileOptions());
        runnable.setType(args.type);
        return runnable;
    }

    @Override
    protected String taskName(RiotStep<?, ?> step) {
        return "Importing";
    }

}
