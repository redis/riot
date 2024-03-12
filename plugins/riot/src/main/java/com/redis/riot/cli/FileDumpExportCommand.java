package com.redis.riot.cli;

import com.redis.riot.file.FileDumpExport;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "file-export", description = "Export Redis data to JSON or XML files.")
public class FileDumpExportCommand extends AbstractExportCommand {

    @ArgGroup(exclusive = false)
    FileDumpExportArgs args = new FileDumpExportArgs();

    @Override
    protected FileDumpExport exportExecutable() {
        FileDumpExport executable = new FileDumpExport();
        executable.setFile(args.file);
        executable.setAppend(args.append);
        executable.setElementName(args.elementName);
        executable.setLineSeparator(args.lineSeparator);
        executable.setRootName(args.rootName);
        executable.setFileOptions(args.fileOptions());
        executable.setType(args.type);
        return executable;
    }

}
