package com.redis.riot.cli;

import com.redis.riot.core.file.FileDumpExport;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "file-export", description = "Export Redis data to JSON or XML files.")
public class FileDumpExportCommand extends AbstractExportCommand {

    // private static final String TASK_NAME = "Exporting to file %s";

    @Parameters(arity = "1", description = "File path or URL", paramLabel = "FILE")
    private String file;

    @ArgGroup(exclusive = false, heading = "File options%n")
    private FileDumpArgs fileDumpArgs = new FileDumpArgs();

    @Option(names = "--append", description = "Append to file if it exists.")
    private boolean append;

    @Option(names = "--root", description = "XML root element tag name (default: ${DEFAULT-VALUE}).", paramLabel = "<string>")
    private String rootName = FileDumpExport.DEFAULT_ROOT_NAME;

    @Option(names = "--element", description = "XML element tag name (default: ${DEFAULT-VALUE}).", paramLabel = "<string>")
    private String elementName = FileDumpExport.DEFAULT_ELEMENT_NAME;

    @Option(names = "--line-sep", description = "String to separate lines (default: system default).", paramLabel = "<string>")
    private String lineSeparator = FileDumpExport.DEFAULT_LINE_SEPARATOR;

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
    }

    @Override
    protected FileDumpExport getExportExecutable() {
        FileDumpExport executable = new FileDumpExport(redisClient(), file);
        executable.setAppend(append);
        executable.setElementName(elementName);
        executable.setLineSeparator(lineSeparator);
        executable.setRootName(rootName);
        executable.setFileOptions(fileDumpArgs.fileOptions());
        executable.setType(fileDumpArgs.getType());
        return executable;
    }

}
