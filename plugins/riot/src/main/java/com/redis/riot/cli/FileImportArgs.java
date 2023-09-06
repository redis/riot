package com.redis.riot.cli;

import java.util.List;

import com.redis.riot.file.FileImport;
import com.redis.riot.file.FileType;

import picocli.CommandLine.Option;

public class FileImportArgs extends FileArgs {

    @Option(names = { "-t", "--filetype" }, description = "File type: ${COMPLETION-CANDIDATES}.", paramLabel = "<type>")
    FileType fileType;

    @Option(names = "--max", description = "Max number of lines to import.", paramLabel = "<count>")
    Integer maxItemCount;

    @Option(names = "--fields", arity = "1..*", description = "Delimited/FW field names.", paramLabel = "<names>")
    List<String> fields;

    @Option(names = { "-h", "--header" }, description = "Delimited/FW first line contains field names.")
    boolean header;

    @Option(names = "--header-line", description = "Index of header line.", paramLabel = "<index>")
    Integer headerLine;

    @Option(names = "--delimiter", description = "Delimiter character.", paramLabel = "<string>")
    String delimiter;

    @Option(names = "--skip", description = "Delimited/FW lines to skip at start.", paramLabel = "<count>")
    Integer linesToSkip;

    @Option(names = "--include", arity = "1..*", description = "Delimited/FW field indices to include (0-based).", paramLabel = "<index>")
    int[] includedFields;

    @Option(names = "--ranges", arity = "1..*", description = "Fixed-width column ranges.", paramLabel = "<string>")
    List<String> columnRanges;

    @Option(names = "--quote", description = "Escape character for delimited files (default: ${DEFAULT-VALUE}).", paramLabel = "<char>")
    Character quoteCharacter = FileImport.DEFAULT_QUOTE_CHARACTER;

    @Option(names = "--cont", description = "Line continuation string (default: ${DEFAULT-VALUE}).", paramLabel = "<string>")
    String continuationString = FileImport.DEFAULT_CONTINUATION_STRING;

}
