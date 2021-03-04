package com.redislabs.riot.file;

import lombok.*;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.Range;
import picocli.CommandLine.Option;

import java.util.ArrayList;
import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class FileImportOptions {

    @Getter
    @Builder.Default
    @Option(names = "--fields", arity = "1..*", description = "Delimited/FW field names", paramLabel = "<names>")
    private List<String> names = new ArrayList<>();
    @Option(names = {"-h", "--header"}, description = "Delimited/FW first line contains field names")
    private boolean header;
    @Option(names = "--delimiter", description = "Delimiter character", paramLabel = "<string>")
    private String delimiter;
    @Option(names = "--skip", description = "Delimited/FW lines to skip at start", paramLabel = "<count>")
    private Integer linesToSkip;
    @Getter
    @Builder.Default
    @Option(names = "--include", arity = "1..*", description = "Delimited/FW field indices to include (0-based)", paramLabel = "<index>")
    private List<Integer> includedFields = new ArrayList<>();
    @Getter
    @Builder.Default
    @Option(names = "--ranges", arity = "1..*", description = "Fixed-width column ranges", paramLabel = "<int>")
    private List<Range> columnRanges = new ArrayList<>();
    @Getter
    @Builder.Default
    @Option(names = "--quote", description = "Escape character for delimited files (default: ${DEFAULT-VALUE})", paramLabel = "<char>")
    private Character quoteCharacter = DelimitedLineTokenizer.DEFAULT_QUOTE_CHARACTER;
    @Getter
    @Builder.Default
    @Option(names = "--continuation", description = "Line continuation string (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
    private String continuationString = "\\";

    public String delimiter(String file) {
        if (delimiter == null) {
            String extension = FileUtils.extension(file);
            if (extension != null) {
                switch (extension) {
                    case FileUtils.EXT_TSV:
                        return DelimitedLineTokenizer.DELIMITER_TAB;
                    case FileUtils.EXT_CSV:
                        return DelimitedLineTokenizer.DELIMITER_COMMA;
                }
            }
            return DelimitedLineTokenizer.DELIMITER_COMMA;
        }
        return delimiter;
    }

    public int linesToSkip() {
        if (linesToSkip == null) {
            if (header) {
                return 1;
            }
            return 0;
        }
        return linesToSkip;
    }

}
