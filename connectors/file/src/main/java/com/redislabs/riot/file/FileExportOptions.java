package com.redislabs.riot.file;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.batch.item.file.FlatFileItemWriter;
import picocli.CommandLine;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FileExportOptions {

    @CommandLine.Option(names = "--append", description = "Append to file if it exists")
    private boolean append;
    @Builder.Default
    @CommandLine.Option(names = "--root", description = "XML root element tag name (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
    private String rootName = "root";
    @Builder.Default
    @CommandLine.Option(names = "--element", description = "XML element tag name (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
    private String elementName = "record";
    @Builder.Default
    @CommandLine.Option(names = "--line-sep", description = "String to separate lines (default: system default)", paramLabel = "<string>")
    private String lineSeparator = FlatFileItemWriter.DEFAULT_LINE_SEPARATOR;

}
