package com.redislabs.riot.file;

import lombok.Data;
import org.springframework.batch.item.file.FlatFileItemWriter;
import picocli.CommandLine;

@Data
public class FileExportOptions {

    @CommandLine.Option(names = "--append", description = "Append to file if it exists")
    private boolean append;
    @CommandLine.Option(names = "--root", description = "XML root element tag name (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
    private String rootName = "root";
    @CommandLine.Option(names = "--element", description = "XML element tag name (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
    private String elementName = "record";
    @CommandLine.Option(names = "--line-sep", description = "String to separate lines (default: system default)", paramLabel = "<string>")
    private String lineSeparator = FlatFileItemWriter.DEFAULT_LINE_SEPARATOR;

}
