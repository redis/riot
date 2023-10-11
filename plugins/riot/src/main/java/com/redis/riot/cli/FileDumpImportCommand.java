package com.redis.riot.cli;

import java.util.List;

import com.redis.riot.core.RiotStep;
import com.redis.riot.file.FileDumpImport;
import com.redis.riot.file.FileDumpType;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "dump-import", description = "Import Redis data files into Redis.")
public class FileDumpImportCommand extends AbstractStructImportCommand {

    @Parameters(arity = "0..*", description = "One ore more files or URLs", paramLabel = "FILE")
    List<String> files;

    @Option(names = { "-t", "--filetype" }, description = "File type: ${COMPLETION-CANDIDATES}.", paramLabel = "<type>")
    FileDumpType type;

    @ArgGroup(exclusive = false, heading = "File options%n")
    FileArgs args = new FileArgs();

    @Override
    protected FileDumpImport getKeyValueImportExecutable() {
        FileDumpImport executable = new FileDumpImport();
        executable.setFiles(files);
        executable.setFileOptions(args.fileOptions());
        executable.setType(type);
        return executable;
    }

    @Override
    protected String taskName(RiotStep<?, ?> step) {
        return "Importing";
    }

}
