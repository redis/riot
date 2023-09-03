package com.redis.riot.cli;

import java.util.List;

import com.redis.riot.core.StepBuilder;
import com.redis.riot.file.FileDumpImport;
import com.redis.spring.batch.util.BatchUtils;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "dump-import", description = "Import Redis data files into Redis.")
public class FileDumpImportCommand extends AbstractKeyValueImportCommand {

    @Parameters(arity = "0..*", description = "One ore more files or URLs", paramLabel = "FILE")
    private List<String> files;

    @ArgGroup(exclusive = false, heading = "File options%n")
    private FileDumpArgs fileDumpArgs = new FileDumpArgs();

    public List<String> getFiles() {
        return files;
    }

    public void setFiles(List<String> files) {
        this.files = files;
    }

    @Override
    protected FileDumpImport getKeyValueImportExecutable() {
        FileDumpImport executable = new FileDumpImport(redisClient(), files);
        executable.setFileOptions(fileDumpArgs.fileOptions());
        executable.setType(fileDumpArgs.getType());
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
