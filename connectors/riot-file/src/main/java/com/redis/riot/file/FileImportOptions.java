package com.redis.riot.file;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;

public class FileImportOptions {

	public static final String DEFAULT_CONTINUATION_STRING = "\\";
	public static final Character DEFAULT_QUOTE_CHARACTER = DelimitedLineTokenizer.DEFAULT_QUOTE_CHARACTER;

	private Collection<String> files = new ArrayList<>();
	private FileOptions fileOptions = new FileOptions();
	private FileType fileType;
	private Integer maxItemCount;
	private List<String> fields;
	private boolean header;
	private Integer headerLine;
	private String delimiter;
	private Integer linesToSkip;
	private int[] includedFields;
	private List<String> columnRanges;
	private Character quoteCharacter = DEFAULT_QUOTE_CHARACTER;
	private String continuationString = DEFAULT_CONTINUATION_STRING;
	private Map<String, Pattern> regexes;

	public Collection<String> getFiles() {
		return files;
	}

	public void setFiles(Collection<String> files) {
		this.files = files;
	}

	public FileOptions getFileOptions() {
		return fileOptions;
	}

	public void setFileOptions(FileOptions fileOptions) {
		this.fileOptions = fileOptions;
	}

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

	public Map<String, Pattern> getRegexes() {
		return regexes;
	}

	public void setRegexes(Map<String, Pattern> regexes) {
		this.regexes = regexes;
	}

}
