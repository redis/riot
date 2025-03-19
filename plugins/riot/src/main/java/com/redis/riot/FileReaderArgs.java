package com.redis.riot;

import java.util.List;
import java.util.Set;

import com.redis.riot.file.ReadOptions;

import lombok.ToString;
import picocli.CommandLine.Option;

@ToString
public class FileReaderArgs extends FileArgs {

	@Option(names = "--header", description = "Use first line as field names for CSV/fixed-length files")
	private boolean header;

	@Option(names = "--ranges", arity = "1..*", description = "Column ranges for fixed-length files.", paramLabel = "<string>")
	private List<String> columnRanges;

	@Option(names = "--cont", description = "Line continuation string (default: ${DEFAULT-VALUE}).", paramLabel = "<string>")
	private String continuationString = ReadOptions.DEFAULT_CONTINUATION_STRING;

	@Option(names = "--fields", arity = "1..*", description = "Delimited/FW field names.", paramLabel = "<names>")
	private List<String> fields;

	@Option(names = "--header-line", description = "Index of header line.", paramLabel = "<index>")
	private Integer headerLine;

	@Option(names = "--include", arity = "1..*", description = "Field indices to include in CSV/fixed-length files (0-based).", paramLabel = "<index>")
	private Set<Integer> includedFields;

	@Option(names = "--skip-lines", description = "Lines to skip at start of CSV/fixed-length files.", paramLabel = "<count>")
	private Integer linesToSkip;

	@Option(names = "--max", description = "Max number of lines to import.", paramLabel = "<count>")
	private int maxItemCount;

	public ReadOptions readOptions() {
		ReadOptions options = new ReadOptions();
		apply(options);
		options.setHeader(header);
		options.setColumnRanges(columnRanges);
		options.setContinuationString(continuationString);
		options.setFields(fields);
		options.setHeaderLine(headerLine);
		options.setIncludedFields(includedFields);
		options.setLinesToSkip(linesToSkip);
		options.setMaxItemCount(maxItemCount);
		return options;
	}

	public boolean isHeader() {
		return header;
	}

	public void setHeader(boolean header) {
		this.header = header;
	}

	public List<String> getColumnRanges() {
		return columnRanges;
	}

	public void setColumnRanges(List<String> columnRanges) {
		this.columnRanges = columnRanges;
	}

	public String getContinuationString() {
		return continuationString;
	}

	public void setContinuationString(String continuationString) {
		this.continuationString = continuationString;
	}

	public List<String> getFields() {
		return fields;
	}

	public void setFields(List<String> fields) {
		this.fields = fields;
	}

	public Integer getHeaderLine() {
		return headerLine;
	}

	public void setHeaderLine(Integer headerLine) {
		this.headerLine = headerLine;
	}

	public Set<Integer> getIncludedFields() {
		return includedFields;
	}

	public void setIncludedFields(Set<Integer> fields) {
		this.includedFields = fields;
	}

	public Integer getLinesToSkip() {
		return linesToSkip;
	}

	public void setLinesToSkip(Integer linesToSkip) {
		this.linesToSkip = linesToSkip;
	}

	public int getMaxItemCount() {
		return maxItemCount;
	}

	public void setMaxItemCount(int maxItemCount) {
		this.maxItemCount = maxItemCount;
	}

}
