package com.redis.riot.cli.file;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;

import picocli.CommandLine.Option;

public class FlatFileOptions {

	public static final String DEFAULT_CONTINUATION_STRING = "\\";
	public static final Character DEFAULT_QUOTE_CHARACTER = DelimitedLineTokenizer.DEFAULT_QUOTE_CHARACTER;

	@Option(names = "--max", description = "Max number of lines to import.", paramLabel = "<count>")
	private int maxItemCount;
	@Option(names = "--fields", arity = "1..*", description = "Delimited/FW field names.", paramLabel = "<names>")
	private List<String> fields = new ArrayList<>();
	@Option(names = { "-h", "--header" }, description = "Delimited/FW first line contains field names.")
	private boolean header;
	@Option(names = "--header-line", description = "Index of header line.", paramLabel = "<index>")
	private Optional<Integer> headerLine = Optional.empty();
	@Option(names = "--delimiter", description = "Delimiter character.", paramLabel = "<string>")
	private Optional<String> delimiter = Optional.empty();
	@Option(names = "--skip", description = "Delimited/FW lines to skip at start.", paramLabel = "<count>")
	private int linesToSkip;
	@Option(names = "--include", arity = "1..*", description = "Delimited/FW field indices to include (0-based).", paramLabel = "<index>")
	private int[] includedFields;
	@Option(names = "--ranges", arity = "1..*", description = "Fixed-width column ranges.", paramLabel = "<string>")
	private List<String> columnRanges = new ArrayList<>();
	@Option(names = "--quote", description = "Escape character for delimited files (default: ${DEFAULT-VALUE}).", paramLabel = "<char>")
	private Character quoteCharacter = DEFAULT_QUOTE_CHARACTER;
	@Option(names = "--cont", description = "Line continuation string (default: ${DEFAULT-VALUE}).", paramLabel = "<string>")
	private String continuationString = DEFAULT_CONTINUATION_STRING;

	public FlatFileOptions() {
	}

	private FlatFileOptions(Builder builder) {
		this.maxItemCount = builder.maxItemCount;
		this.fields = builder.names;
		this.header = builder.header;
		this.headerLine = builder.headerLine;
		this.delimiter = builder.delimiter;
		this.linesToSkip = builder.linesToSkip;
		this.includedFields = builder.includedFields;
		this.columnRanges = builder.columnRanges;
		this.quoteCharacter = builder.quoteCharacter;
		this.continuationString = builder.continuationString;
	}

	public int getMaxItemCount() {
		return maxItemCount;
	}

	public void setMaxItemCount(int count) {
		this.maxItemCount = count;
	}

	public Collection<String> getFields() {
		return fields;
	}

	public void setFields(Collection<String> names) {
		this.fields = new ArrayList<>(names);
	}

	public void setFields(String... names) {
		this.fields = Arrays.asList(names);
	}

	public boolean isHeader() {
		return header;
	}

	public void setHeader(boolean header) {
		this.header = header;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = Optional.of(delimiter);
	}

	public Optional<Integer> getHeaderLine() {
		return headerLine;
	}

	public void setHeaderLine(int lineIndex) {
		this.headerLine = Optional.of(lineIndex);
	}

	public int getLinesToSkip() {
		return linesToSkip;
	}

	public void setLinesToSkip(int linesToSkip) {
		this.linesToSkip = linesToSkip;
	}

	public int[] getIncludedFields() {
		return includedFields;
	}

	public void setIncludedFields(int... includedFields) {
		this.includedFields = includedFields;
	}

	public List<String> getColumnRanges() {
		return columnRanges;
	}

	public void setColumnRanges(String... columnRanges) {
		this.columnRanges = Arrays.asList(columnRanges);
	}

	public Optional<String> getDelimiter() {
		return delimiter;
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

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder extends FileOptions.Builder<Builder> {
		private int maxItemCount;
		private List<String> names = new ArrayList<>();
		private boolean header;
		private Optional<Integer> headerLine = Optional.empty();
		private Optional<String> delimiter = Optional.empty();
		private int linesToSkip;
		private int[] includedFields;
		private List<String> columnRanges = new ArrayList<>();
		private Character quoteCharacter = DelimitedLineTokenizer.DEFAULT_QUOTE_CHARACTER;
		private String continuationString = DEFAULT_CONTINUATION_STRING;

		private Builder() {
		}

		public Builder headerLine(int lineIndex) {
			return headerLine(Optional.of(lineIndex));
		}

		public Builder headerLine(Optional<Integer> lineIndex) {
			this.headerLine = lineIndex;
			return this;
		}

		public Builder max(int count) {
			this.maxItemCount = count;
			return this;
		}

		public Builder names(String... names) {
			this.names = Arrays.asList(names);
			return this;
		}

		public Builder header(boolean header) {
			this.header = header;
			return this;
		}

		public Builder delimiter(String delimiter) {
			return delimiter(Optional.of(delimiter));
		}

		public Builder delimiter(Optional<String> delimiter) {
			this.delimiter = delimiter;
			return this;
		}

		public Builder linesToSkip(int linesToSkip) {
			this.linesToSkip = linesToSkip;
			return this;
		}

		public Builder includedFields(int... includedFields) {
			this.includedFields = includedFields;
			return this;
		}

		public Builder columnRanges(String... columnRanges) {
			this.columnRanges = Arrays.asList(columnRanges);
			return this;
		}

		public Builder quoteCharacter(Character quoteCharacter) {
			this.quoteCharacter = quoteCharacter;
			return this;
		}

		public Builder continuationString(String continuationString) {
			this.continuationString = continuationString;
			return this;
		}

		public FlatFileOptions build() {
			return new FlatFileOptions(this);
		}
	}

	@Override
	public String toString() {
		return "FlatFileOptions [names=" + fields + ", max=" + maxItemCount + ", header=" + header + ", headerLine="
				+ headerLine + ", delimiter=" + delimiter + ", linesToSkip=" + linesToSkip + ", includedFields="
				+ Arrays.toString(includedFields) + ", columnRanges=" + columnRanges + ", quoteCharacter="
				+ quoteCharacter + ", continuationString=" + continuationString + "]";
	}

}
