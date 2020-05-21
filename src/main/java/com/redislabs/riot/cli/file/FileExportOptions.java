package com.redislabs.riot.cli.file;

import lombok.Getter;
import org.springframework.batch.item.file.FlatFileItemWriter;
import picocli.CommandLine.Option;

import java.util.Locale;

public class FileExportOptions extends FileOptions {

    @Getter
    @Option(names = "--append", description = "Append to file if it exists")
    private boolean append;
    @Getter
    @Option(names = "--force-sync", description = "Force-sync changes to disk on flush")
    private boolean forceSync;
    @Getter
    @Option(names = "--line-sep", description = "String to separate lines (default: system default)", paramLabel = "<string>")
    private String lineSeparator = FlatFileItemWriter.DEFAULT_LINE_SEPARATOR;
    @Getter
    @Option(names = "--format", description = "Format string used to aggregate items", paramLabel = "<string>")
    private String format;
    @Getter
    @Option(names = "--locale", description = "Locale", paramLabel = "<tag>")
    private Locale locale = Locale.ENGLISH;
    @Getter
    @Option(names = "--max-length", description = "Max length of the formatted string", paramLabel = "<int>")
    private Integer maxLength;
    @Getter
    @Option(names = "--min-length", description = "Min length of the formatted string", paramLabel = "<int>")
    private Integer minLength;
    @Getter
    @Option(names = "--root", description = "XML root element tag name", paramLabel = "<string>")
    private String root;

}
