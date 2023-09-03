package com.redis.riot.cli;

import java.util.List;

import com.redis.riot.file.FileImport;
import com.redis.riot.file.FileType;

import picocli.CommandLine.Option;

public class FileImportArgs extends FileArgs {

    @Option(names = { "-t", "--filetype" }, description = "File type: ${COMPLETION-CANDIDATES}.", paramLabel = "<type>")
    private FileType fileType;

    @Option(names = "--max", description = "Max number of lines to import.", paramLabel = "<count>")
    private Integer maxItemCount;

    @Option(names = "--fields", arity = "1..*", description = "Delimited/FW field names.", paramLabel = "<names>")
    private List<String> fields;

    @Option(names = { "-h", "--header" }, description = "Delimited/FW first line contains field names.")
    private boolean header;

    @Option(names = "--header-line", description = "Index of header line.", paramLabel = "<index>")
    private Integer headerLine;

    @Option(names = "--delimiter", description = "Delimiter character.", paramLabel = "<string>")
    private String delimiter;

    @Option(names = "--skip", description = "Delimited/FW lines to skip at start.", paramLabel = "<count>")
    private Integer linesToSkip;

    @Option(names = "--include", arity = "1..*", description = "Delimited/FW field indices to include (0-based).", paramLabel = "<index>")
    private int[] includedFields;

    @Option(names = "--ranges", arity = "1..*", description = "Fixed-width column ranges.", paramLabel = "<string>")
    private List<String> columnRanges;

    @Option(names = "--quote", description = "Escape character for delimited files (default: ${DEFAULT-VALUE}).", paramLabel = "<char>")
    private Character quoteCharacter = FileImport.DEFAULT_QUOTE_CHARACTER;

    @Option(names = "--cont", description = "Line continuation string (default: ${DEFAULT-VALUE}).", paramLabel = "<string>")
    private String continuationString = FileImport.DEFAULT_CONTINUATION_STRING;

    public FileType getFileType() {
        return fileType;
    }

    public void setFileType(FileType fileType) {
        this.fileType = fileType;
    }

    public Integer getMaxItemCount() {
        return maxItemCount;
    }

    public void setMaxItemCount(Integer maxItemCount) {
        this.maxItemCount = maxItemCount;
    }

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    public boolean isHeader() {
        return header;
    }

    public void setHeader(boolean header) {
        this.header = header;
    }

    public Integer getHeaderLine() {
        return headerLine;
    }

    public void setHeaderLine(Integer headerLine) {
        this.headerLine = headerLine;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public Integer getLinesToSkip() {
        return linesToSkip;
    }

    public void setLinesToSkip(Integer linesToSkip) {
        this.linesToSkip = linesToSkip;
    }

    public int[] getIncludedFields() {
        return includedFields;
    }

    public void setIncludedFields(int[] includedFields) {
        this.includedFields = includedFields;
    }

    public List<String> getColumnRanges() {
        return columnRanges;
    }

    public void setColumnRanges(List<String> columnRanges) {
        this.columnRanges = columnRanges;
    }

    public Character getQuoteCharacter() {
        return quoteCharacter;
    }

    public void setQuoteCharacter(Character quoteCharacter) {
        this.quoteCharacter = quoteCharacter;
    }

    public String getContinuationString() {
        return continuationString;
    }

    public void setContinuationString(String continuationString) {
        this.continuationString = continuationString;
    }

}
