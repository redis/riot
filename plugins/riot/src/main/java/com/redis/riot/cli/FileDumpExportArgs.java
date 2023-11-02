package com.redis.riot.cli;

import com.redis.riot.file.FileDumpExport;
import com.redis.riot.file.FileDumpType;

import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

public class FileDumpExportArgs extends FileArgs {

    @Parameters(arity = "1", description = "File path or URL", paramLabel = "FILE")
    String file;

    @Option(names = { "-t", "--filetype" }, description = "File type: ${COMPLETION-CANDIDATES}.", paramLabel = "<type>")
    FileDumpType type;

    @Option(names = "--append", description = "Append to file if it exists.")
    boolean append;

    @Option(names = "--root", description = "XML root element tag name (default: ${DEFAULT-VALUE}).", paramLabel = "<string>")
    String rootName = FileDumpExport.DEFAULT_ROOT_NAME;

    @Option(names = "--element", description = "XML element tag name (default: ${DEFAULT-VALUE}).", paramLabel = "<string>")
    String elementName = FileDumpExport.DEFAULT_ELEMENT_NAME;

    @Option(names = "--line-sep", description = "String to separate lines (default: system default).", paramLabel = "<string>")
    String lineSeparator = FileDumpExport.DEFAULT_LINE_SEPARATOR;

}
