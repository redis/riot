package com.redislabs.riot.cli.file;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.Range;
import picocli.CommandLine.Option;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class FileImportOptions extends FileOptions {

    @Getter
    @Option(names = {"--skip"}, description = "Lines to skip at start of file (default: ${DEFAULT-VALUE})", paramLabel = "<count>")
    private int linesToSkip = 0;
    @Getter
    @Option(names = "--include", arity = "1..*", description = "Field indices to include (0-based)", paramLabel = "<index>")
    private List<Integer> includedFields = new ArrayList<>();
    @Getter
    @Option(names = "--ranges", arity = "1..*", description = "Fixed-width column ranges", paramLabel = "<int>")
    private List<Range> columnRanges = new ArrayList<>();
    @Getter
    @Option(names = {"-q", "--quote"}, description = "Escape character (default: ${DEFAULT-VALUE})", paramLabel = "<char>")
    private Character quoteCharacter = DelimitedLineTokenizer.DEFAULT_QUOTE_CHARACTER;

}
