package com.redis.riot.cli;

import java.util.List;

import com.redis.riot.core.file.FileDumpImport;

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

}
