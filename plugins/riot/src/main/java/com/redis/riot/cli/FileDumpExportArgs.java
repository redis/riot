package com.redis.riot.cli;

import com.redis.riot.file.FileDumpExport;

import picocli.CommandLine.Option;

public class FileDumpExportArgs extends FileDumpArgs {

    @Option(names = "--append", description = "Append to file if it exists.")
    boolean append;

    @Option(names = "--root", description = "XML root element tag name (default: ${DEFAULT-VALUE}).", paramLabel = "<string>")
    String rootName = FileDumpExport.DEFAULT_ROOT_NAME;

    @Option(names = "--element", description = "XML element tag name (default: ${DEFAULT-VALUE}).", paramLabel = "<string>")
    String elementName = FileDumpExport.DEFAULT_ELEMENT_NAME;

    @Option(names = "--line-sep", description = "String to separate lines (default: system default).", paramLabel = "<string>")
    String lineSeparator = FileDumpExport.DEFAULT_LINE_SEPARATOR;

}
