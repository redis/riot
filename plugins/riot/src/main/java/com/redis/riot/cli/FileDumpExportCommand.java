package com.redis.riot.cli;

import com.redis.riot.file.FileDumpExport;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "file-export", description = "Export Redis data to JSON or XML files.")
public class FileDumpExportCommand extends AbstractExportCommand {

    @Parameters(arity = "1", description = "File path or URL", paramLabel = "FILE")
    String file;

    @ArgGroup(exclusive = false)
    FileDumpExportArgs args = new FileDumpExportArgs();

    @Override
    protected FileDumpExport getExportExecutable() {
        FileDumpExport executable = new FileDumpExport(redisClient(), file);
        executable.setAppend(args.append);
        executable.setElementName(args.elementName);
        executable.setLineSeparator(args.lineSeparator);
        executable.setRootName(args.rootName);
        executable.setFileOptions(args.fileOptions());
        executable.setType(args.type);
        return executable;
    }

}
