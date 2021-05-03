package com.redislabs.riot.file;

import lombok.*;
import lombok.experimental.Accessors;
import org.springframework.batch.item.file.FlatFileItemWriter;
import picocli.CommandLine;

@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
public class FileExportOptions extends FileOptions {

    public static final String DEFAULT_ELEMENT_NAME = "record";
    public static final String DEFAULT_ROOT_NAME = "root";

    @CommandLine.Option(names = {"-t", "--filetype"}, description = "File type: ${COMPLETION-CANDIDATES}", paramLabel = "<type>")
    private DumpFileType type;
    @CommandLine.Option(names = "--append", description = "Append to file if it exists")
    private boolean append;
    @CommandLine.Option(names = "--root", description = "XML root element tag name (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
    private String rootName = DEFAULT_ROOT_NAME;
    @CommandLine.Option(names = "--element", description = "XML element tag name (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
    private String elementName = DEFAULT_ELEMENT_NAME;
    @CommandLine.Option(names = "--line-sep", description = "String to separate lines (default: system default)", paramLabel = "<string>")
    private String lineSeparator = FlatFileItemWriter.DEFAULT_LINE_SEPARATOR;

    public static FileExportOptionsBuilder builder() {
        return new FileExportOptionsBuilder();
    }

    @Setter
    @Accessors(fluent = true)
    public static class FileExportOptionsBuilder extends FileOptionsBuilder<FileExportOptionsBuilder> {

        private DumpFileType type;
        private boolean append;
        private String rootName = DEFAULT_ROOT_NAME;
        private String elementName = DEFAULT_ELEMENT_NAME;
        private String lineSeparator = FlatFileItemWriter.DEFAULT_LINE_SEPARATOR;

        public FileExportOptions build() {
            return build(new FileExportOptions(type, append, rootName, elementName, lineSeparator));
        }

    }

}
