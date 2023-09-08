package com.redis.riot.cli;

import java.util.List;

import com.redis.riot.core.StepBuilder;
import com.redis.riot.file.FileDumpImport;
import com.redis.riot.file.FileDumpType;
import com.redis.spring.batch.util.BatchUtils;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "dump-import", description = "Import Redis data files into Redis.")
public class FileDumpImportCommand extends AbstractKeyValueImportCommand {

    @Parameters(arity = "0..*", description = "One ore more files or URLs", paramLabel = "FILE")
    List<String> files;

    @Option(names = { "-t", "--filetype" }, description = "File type: ${COMPLETION-CANDIDATES}.", paramLabel = "<type>")
    FileDumpType type;

    @ArgGroup(exclusive = false, heading = "File options%n")
    FileArgs args = new FileArgs();

    @Override
    protected FileDumpImport getKeyValueImportExecutable() {
        FileDumpImport executable = new FileDumpImport(redisClient(), files);
        executable.setFileOptions(args.fileOptions());
        executable.setType(type);
        return executable;
    }

    @Override
    protected String taskName(StepBuilder<?, ?> step) {
        return "Importing";
    }

    @Override
    protected long size(StepBuilder<?, ?> step) {
        return BatchUtils.SIZE_UNKNOWN;
    }

}
