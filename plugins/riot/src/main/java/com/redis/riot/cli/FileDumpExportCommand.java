package com.redis.riot.cli;

import com.redis.riot.file.FileDumpExport;
import com.redis.riot.file.FileDumpType;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "file-export", description = "Export Redis data to JSON or XML files.")
public class FileDumpExportCommand extends AbstractExportCommand {

    @Parameters(arity = "1", description = "File path or URL", paramLabel = "FILE")
    String file;

    @Option(names = { "-t", "--filetype" }, description = "File type: ${COMPLETION-CANDIDATES}.", paramLabel = "<type>")
    FileDumpType type;

    @ArgGroup(exclusive = false, heading = "File options%n")
    FileArgs fileArgs = new FileArgs();

    @Option(names = "--append", description = "Append to file if it exists.")
    boolean append;

    @Option(names = "--root", description = "XML root element tag name (default: ${DEFAULT-VALUE}).", paramLabel = "<string>")
    String rootName = FileDumpExport.DEFAULT_ROOT_NAME;

    @Option(names = "--element", description = "XML element tag name (default: ${DEFAULT-VALUE}).", paramLabel = "<string>")
    String elementName = FileDumpExport.DEFAULT_ELEMENT_NAME;

    @Option(names = "--line-sep", description = "String to separate lines (default: system default).", paramLabel = "<string>")
    String lineSeparator = FileDumpExport.DEFAULT_LINE_SEPARATOR;

    @Override
    protected FileDumpExport getExportExecutable() {
        FileDumpExport executable = new FileDumpExport(redisClient(), file);
        executable.setAppend(append);
        executable.setElementName(elementName);
        executable.setLineSeparator(lineSeparator);
        executable.setRootName(rootName);
        executable.setFileOptions(fileArgs.fileOptions());
        executable.setType(type);
        return executable;
    }

}
