package com.redis.riot.file;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonDeserializer;

public class FileReaderOptions {

	public static final String DEFAULT_CONTINUATION_STRING = "\\";

	private FileOptions fileOptions = new FileOptions();
	private OptionalInt maxItemCount = OptionalInt.empty();
	private Set<Integer> includedFields;
	private String continuationString = DEFAULT_CONTINUATION_STRING;
	private List<String> fields;
	private Integer headerLine;
	private Integer linesToSkip;
	private List<String> columnRanges;
	@SuppressWarnings("rawtypes")
	private final Map<Class, JsonDeserializer> deserializers = new LinkedHashMap<>();
	private Class<?> itemType = Map.class;

	public FileOptions getFileOptions() {
		return fileOptions;
	}

	public void setFileOptions(FileOptions fileOptions) {
		this.fileOptions = fileOptions;
	}

	public <T> void addDeserializer(Class<T> type, JsonDeserializer<? extends T> deserializer) {
		deserializers.put(type, deserializer);
	}

	@SuppressWarnings("rawtypes")
	public Map<Class, JsonDeserializer> getDeserializers() {
		return deserializers;
	}

	public Class<?> getItemType() {
		return itemType;
	}

	public void setItemType(Class<?> type) {
		this.itemType = type;
	}

	public OptionalInt getMaxItemCount() {
		return maxItemCount;
	}

	public void setMaxItemCount(int count) {
		this.maxItemCount = count > 0 ? OptionalInt.of(count) : OptionalInt.empty();
	}

	public Set<Integer> getIncludedFields() {
		return includedFields;
	}

	public void setIncludedFields(Set<Integer> fields) {
		this.includedFields = fields;
	}

	public String getContinuationString() {
		return continuationString;
	}

	public void setContinuationString(String string) {
		this.continuationString = string;
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

	public Integer getLinesToSkip() {
		return linesToSkip;
	}

	public void setLinesToSkip(Integer linesToSkip) {
		this.linesToSkip = linesToSkip;
	}

	public List<String> getColumnRanges() {
		return columnRanges;
	}

	public void setColumnRanges(List<String> columnRanges) {
		this.columnRanges = columnRanges;
	}

}
