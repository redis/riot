package com.redislabs.riot.file;

import lombok.*;
import lombok.experimental.Accessors;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import picocli.CommandLine.Option;

@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
public class FileImportOptions extends FileOptions {

    public static final String DEFAULT_CONTINUATION_STRING = "\\";

    @Option(names = "--fields", arity = "1..*", description = "Delimited/FW field names", paramLabel = "<names>")
    private String[] names;
    @Option(names = {"-h", "--header"}, description = "Delimited/FW first line contains field names")
    private boolean header;
    @Option(names = "--delimiter", description = "Delimiter character", paramLabel = "<string>")
    private String delimiter;
    @Option(names = "--skip", description = "Delimited/FW lines to skip at start", paramLabel = "<count>")
    private Integer linesToSkip;
    @Option(names = "--include", arity = "1..*", description = "Delimited/FW field indices to include (0-based)", paramLabel = "<index>")
    private int[] includedFields;
    @Option(names = "--ranges", arity = "1..*", description = "Fixed-width column ranges", paramLabel = "<string>")
    private String[] columnRanges;
    @Option(names = "--quote", description = "Escape character for delimited files (default: ${DEFAULT-VALUE})", paramLabel = "<char>")
    private Character quoteCharacter = DelimitedLineTokenizer.DEFAULT_QUOTE_CHARACTER;
    @Option(names = "--cont", description = "Line continuation string (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
    private String continuationString = DEFAULT_CONTINUATION_STRING;

    public int linesToSkip() {
        if (linesToSkip == null) {
            if (header) {
                return 1;
            }
            return 0;
        }
        return linesToSkip;
    }

    public static FileImportOptionsBuilder builder() {
        return new FileImportOptionsBuilder();
    }

    @Setter
    @Accessors(fluent = true)
    public static class FileImportOptionsBuilder extends FileOptionsBuilder<FileImportOptionsBuilder> {

        private String[] names;
        private boolean header;
        private String delimiter;
        private Integer linesToSkip;
        private int[] includedFields;
        private String[] columnRanges;
        private Character quoteCharacter = DelimitedLineTokenizer.DEFAULT_QUOTE_CHARACTER;
        private String continuationString = DEFAULT_CONTINUATION_STRING;

        public FileImportOptions build() {
            return build(new FileImportOptions(names, header, delimiter, linesToSkip, includedFields, columnRanges, quoteCharacter, continuationString));
        }

    }
}
