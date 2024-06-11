package com.redis.riot.file;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import org.springframework.core.io.Resource;

import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

public class FileReaderArgs extends FileArgs {

	public static final String DEFAULT_CONTINUATION_STRING = "\\";
	public static final int DEFAULT_MAX_ITEM_COUNT = Integer.MAX_VALUE;

	@Parameters(arity = "1..*", description = "Files or URLs to import. Use '-' to read from stdin.", paramLabel = "FILE")
	private List<String> files;

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
	public Resource resource(String location) {
		if (FileUtils.isStdin(location)) {
			return new FilenameInputStreamResource(System.in, "stdin", "Standard Input");
		}
		Resource resource;
		try {
			resource = super.resource(location);
		} catch (IOException e) {
			throw new RuntimeIOException("Could not create resource for file " + location, e);
		}
		InputStream inputStream;
		try {
			inputStream = resource.getInputStream();
		} catch (IOException e) {
			throw new RuntimeIOException("Could not open input stream for resource " + resource, e);
		}
		if (isGzipped() || FileUtils.isGzip(location)) {
			GZIPInputStream gzipInputStream;
			try {
				gzipInputStream = new GZIPInputStream(inputStream);
			} catch (IOException e) {
				throw new RuntimeIOException("Could not create gzip input stream for resource " + resource, e);
			}
			return new FilenameInputStreamResource(gzipInputStream, resource.getFilename(), resource.getDescription());
		}
		return resource;
	}

	public List<Resource> resources() {
		return files.stream().flatMap(FileUtils::expand).map(this::resource).collect(Collectors.toList());
	}

	public List<String> getFiles() {
		return files;
	}

	public void setFiles(String... files) {
		setFiles(Arrays.asList(files));
	}

	public void setFiles(List<String> files) {
		this.files = files;
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

	@Override
	public String toString() {
		return "FileReaderArgs [files=" + files + ", " + super.toString() + ", columnRanges=" + columnRanges
				+ ", continuationString=" + continuationString + ", fields=" + fields + ", headerLine=" + headerLine
				+ ", includedFields=" + includedFields + ", linesToSkip=" + linesToSkip + ", maxItemCount="
				+ maxItemCount + "]";
	}

}
