package com.redis.riot.file;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.springframework.core.io.Resource;

import picocli.CommandLine.Option;

public class FileReaderArgs extends FileArgs {

	public static final String DEFAULT_CONTINUATION_STRING = "\\";
	public static final int DEFAULT_MAX_ITEM_COUNT = Integer.MAX_VALUE;

	@Option(names = "--ranges", arity = "1..*", description = "Column ranges for fixed-length files.", paramLabel = "<string>")
	private List<String> columnRanges;

	@Option(names = "--cont", description = "Line continuation string (default: ${DEFAULT-VALUE}).", paramLabel = "<string>")
	private String continuationString = DEFAULT_CONTINUATION_STRING;

	@Option(names = "--fields", arity = "1..*", description = "Delimited/FW field names.", paramLabel = "<names>")
	private List<String> fields;

	@Option(names = "--header-line", description = "Index of header line.", paramLabel = "<index>")
	private Integer headerLine;

	@Option(names = "--include", arity = "1..*", description = "Field indices to include in CSV/fixed-length files (0-based).", paramLabel = "<index>")
	private Set<Integer> includedFields;

	@Option(names = "--skip-lines", description = "Lines to skip at start of CSV/fixed-length files.", paramLabel = "<count>")
	private Integer linesToSkip;

	@Option(names = "--max", description = "Max number of lines to import.", paramLabel = "<count>")
	private int maxItemCount = DEFAULT_MAX_ITEM_COUNT;

	@Override
	public Resource resource(String location) throws IOException {
		Resource resource = super.resource(location);
		InputStream inputStream = resource.getInputStream();
		if (isGzipped() || FileUtils.isGzip(location)) {
			return new FilenameInputStreamResource(new GZIPInputStream(inputStream), resource.getFilename(),
					resource.getDescription());
		}
		return resource;
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
