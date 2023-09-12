package com.redis.riot.cli;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.redis.riot.core.AbstractMapImport;
import com.redis.riot.core.StepBuilder;
import com.redis.riot.file.FileImport;
import com.redis.riot.file.FileType;
import com.redis.spring.batch.util.BatchUtils;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "file-import", description = "Import from CSV/JSON/XML files.")
public class FileImportCommand extends AbstractImportCommand {

    @Parameters(arity = "0..*", description = "One ore more files or URLs", paramLabel = "FILE")
    List<String> files = new ArrayList<>();

    @ArgGroup(exclusive = false, heading = "File options%n")
    FileArgs fileArgs = new FileArgs();

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

    @Option(arity = "1..*", names = "--regex", description = "Regular expressions in the form field1=\"regex\" field2=\"regex\"...", paramLabel = "<f=rex>")
    Map<String, Pattern> regexes;

    @Override
    protected AbstractMapImport getMapImportExecutable() {
        FileImport executable = new FileImport();
        executable.setFiles(files);
        executable.setColumnRanges(columnRanges);
        executable.setContinuationString(continuationString);
        executable.setDelimiter(delimiter);
        executable.setFields(fields);
        executable.setFileOptions(fileArgs.fileOptions());
        executable.setFileType(fileType);
        executable.setHeader(header);
        executable.setHeaderLine(headerLine);
        executable.setIncludedFields(includedFields);
        executable.setLinesToSkip(linesToSkip);
        executable.setMaxItemCount(maxItemCount);
        executable.setQuoteCharacter(quoteCharacter);
        executable.setRegexes(regexes);
        return executable;
    }

    @Override
    protected long size(StepBuilder<?, ?> step) {
        return BatchUtils.SIZE_UNKNOWN;
    }

}
