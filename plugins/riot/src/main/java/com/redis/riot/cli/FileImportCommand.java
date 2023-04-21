package com.redis.riot.cli;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.LineCallbackHandler;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.AbstractLineTokenizer;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.item.file.transform.FixedLengthTokenizer;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.batch.item.file.transform.RangeArrayPropertyEditor;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.redis.riot.core.FileExtension;
import com.redis.riot.core.FileType;
import com.redis.riot.core.FileUtils;
import com.redis.riot.core.FileUtils.UnknownFileTypeException;
import com.redis.riot.core.resource.XmlItemReader;
import com.redis.riot.core.ItemReaderIterator;
import com.redis.riot.core.MapFieldSetMapper;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

@Command(name = "file-import", description = "Import data from CSV/JSON/XML files")
public class FileImportCommand extends AbstractImportCommand {

	private static final Logger log = Logger.getLogger(FileImportCommand.class.getName());
	private static final String DELIMITER_PIPE = "|";
	private static final String COMMAND_NAME = "file-import";

	@Mixin
	private FileImportOptions options = new FileImportOptions();
	@ArgGroup(exclusive = false, heading = "Flat file options%n")
	private FlatFileOptions flatFileOptions = new FlatFileOptions();
	@Option(names = { "-t", "--filetype" }, description = "File type: ${COMPLETION-CANDIDATES}.", paramLabel = "<type>")
	private Optional<FileType> fileType = Optional.empty();

	public FileImportCommand() {
	}

	private FileImportCommand(Builder builder) {
		this.flatFileOptions = builder.flatFileOptions;
		this.fileType = builder.fileType;
		this.options = builder.options;
	}

	public Optional<FileType> getFileType() {
		return fileType;
	}

	public void setFileType(FileType fileType) {
		this.fileType = Optional.of(fileType);
	}

	public FileImportOptions getOptions() {
		return options;
	}

	public void setOptions(FileImportOptions options) {
		this.options = options;
	}

	public FlatFileOptions getFlatFileOptions() {
		return flatFileOptions;
	}

	public void setFlatFileOptions(FlatFileOptions options) {
		this.flatFileOptions = options;
	}

	@Override
	protected Job job(JobCommandContext context) {
		Iterator<TaskletStep> stepIterator = options.getResources().map(r -> step(context, r)).iterator();
		SimpleJobBuilder job = context.job(COMMAND_NAME).start(stepIterator.next());
		while (stepIterator.hasNext()) {
			job.next(stepIterator.next());
		}
		return job.build();
	}

	private TaskletStep step(JobCommandContext context, Resource resource) {
		AbstractItemCountingItemStreamItemReader<Map<String, Object>> reader = reader(resource);
		String name = String.join("-", COMMAND_NAME, resource.getDescription());
		if (reader instanceof ItemStreamSupport) {
			((ItemStreamSupport) reader).setName(name);
		}
		ProgressMonitor monitor = progressMonitor().task("Importing " + resource.getFilename()).build();
		return step(step(context, name, reader), monitor).skip(FlatFileParseException.class).build();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private AbstractItemCountingItemStreamItemReader<Map<String, Object>> reader(Resource resource) {
		FileType type = getFileType().orElseGet(() -> type(resource));
		switch (type) {
		case DELIMITED:
			DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
			tokenizer.setDelimiter(
					flatFileOptions.getDelimiter().orElseGet(() -> delimiter(FileUtils.extension(resource))));
			tokenizer.setQuoteCharacter(flatFileOptions.getQuoteCharacter());
			if (!ObjectUtils.isEmpty(flatFileOptions.getIncludedFields())) {
				tokenizer.setIncludedFields(flatFileOptions.getIncludedFields());
			}
			log.log(Level.FINE, "Creating delimited reader with {0} for {1}", new Object[] { options, resource });
			return flatFileReader(resource, tokenizer);
		case FIXED:
			FixedLengthTokenizer fixedLengthTokenizer = new FixedLengthTokenizer();
			RangeArrayPropertyEditor editor = new RangeArrayPropertyEditor();
			Assert.notEmpty(flatFileOptions.getColumnRanges(), "Column ranges are required");
			editor.setAsText(String.join(",", flatFileOptions.getColumnRanges()));
			Range[] ranges = (Range[]) editor.getValue();
			if (ranges.length == 0) {
				throw new IllegalArgumentException("Invalid ranges specified: " + flatFileOptions.getColumnRanges());
			}
			fixedLengthTokenizer.setColumns(ranges);
			log.log(Level.FINE, "Creating fixed-width reader with {0} for {1}", new Object[] { options, resource });
			return flatFileReader(resource, fixedLengthTokenizer);
		case XML:
			log.log(Level.FINE, "Creating XML reader for {0}", resource);
			return (XmlItemReader) FileUtils.xmlReader(resource, Map.class);
		case JSON:
			log.log(Level.FINE, "Creating JSON reader for {0}", resource);
			return (JsonItemReader) FileUtils.jsonReader(resource, Map.class);
		default:
			throw new UnsupportedOperationException("Unsupported file type: " + type);
		}
	}

	public static FileType type(Resource resource) {
		FileExtension extension = FileUtils.extension(resource);
		switch (extension) {
		case FW:
			return FileType.FIXED;
		case JSON:
			return FileType.JSON;
		case XML:
			return FileType.XML;
		case CSV:
		case PSV:
		case TSV:
			return FileType.DELIMITED;
		default:
			throw new UnknownFileTypeException("Unknown file extension: " + extension);
		}
	}

	public static String delimiter(FileExtension extension) {
		switch (extension) {
		case CSV:
			return DelimitedLineTokenizer.DELIMITER_COMMA;
		case PSV:
			return DELIMITER_PIPE;
		case TSV:
			return DelimitedLineTokenizer.DELIMITER_TAB;
		default:
			throw new IllegalArgumentException("Unknown extension: " + extension);
		}
	}

	private FlatFileItemReader<Map<String, Object>> flatFileReader(Resource resource, AbstractLineTokenizer tokenizer) {
		if (!ObjectUtils.isEmpty(flatFileOptions.getNames())) {
			tokenizer.setNames(flatFileOptions.getNames().toArray(new String[0]));
		}
		FlatFileItemReaderBuilder<Map<String, Object>> builder = new FlatFileItemReaderBuilder<>();
		builder.resource(resource);
		builder.encoding(options.getEncoding().name());
		builder.lineTokenizer(tokenizer);
		builder.recordSeparatorPolicy(new DefaultRecordSeparatorPolicy(flatFileOptions.getQuoteCharacter().toString(),
				flatFileOptions.getContinuationString()));
		builder.linesToSkip(linesToSkip());
		builder.strict(true);
		builder.saveState(false);
		builder.fieldSetMapper(new MapFieldSetMapper());
		builder.skippedLinesCallback(new HeaderCallbackHandler(tokenizer, headerIndex()));
		if (flatFileOptions.getMaxItemCount() > 0) {
			builder.maxItemCount(flatFileOptions.getMaxItemCount());
		}
		return builder.build();
	}

	private int headerIndex() {
		Optional<Integer> headerLine = flatFileOptions.getHeaderLine();
		if (headerLine.isPresent()) {
			return headerLine.get();
		}
		return linesToSkip() - 1;
	}

	private int linesToSkip() {
		if (flatFileOptions.getLinesToSkip() > 0) {
			return flatFileOptions.getLinesToSkip();
		}
		if (flatFileOptions.isHeader()) {
			return 1;
		}
		return 0;
	}

	private static class HeaderCallbackHandler implements LineCallbackHandler {

		private final AbstractLineTokenizer tokenizer;
		private final int headerIndex;
		private int lineIndex;

		public HeaderCallbackHandler(AbstractLineTokenizer tokenizer, int headerIndex) {
			this.tokenizer = tokenizer;
			this.headerIndex = headerIndex;
		}

		@Override
		public void handleLine(String line) {
			if (lineIndex == headerIndex) {
				log.log(Level.FINE, "Found header: {0}", line);
				FieldSet fieldSet = tokenizer.tokenize(line);
				List<String> fields = new ArrayList<>();
				for (int index = 0; index < fieldSet.getFieldCount(); index++) {
					fields.add(fieldSet.readString(index));
				}
				tokenizer.setNames(fields.toArray(new String[0]));
			}
			lineIndex++;
		}
	}

	public static Builder builder() {
		return new Builder();
	}

	public static class Builder {

		private FlatFileOptions flatFileOptions = new FlatFileOptions();
		private FileImportOptions options = new FileImportOptions();
		private Optional<FileType> fileType = Optional.empty();

		public Builder flatFileOptions(FlatFileOptions options) {
			Assert.notNull(options, "FlatFileOptions must not be null");
			this.flatFileOptions = options;
			return this;
		}

		public Builder options(FileImportOptions options) {
			Assert.notNull(options, "FileImportOptions must not be null");
			this.options = options;
			return this;
		}

		public Builder fileType(FileType type) {
			return fileType(Optional.of(type));
		}

		public Builder fileType(Optional<FileType> type) {
			this.fileType = type;
			return this;
		}

		public FileImportCommand build() {
			return new FileImportCommand(this);
		}

	}

	/**
	 * 
	 * @param files Files to create readers for
	 * @return List of readers for the given files, which might contain wildcards
	 * @throws IOException
	 */
	public List<ItemReader<Map<String, Object>>> readers(String... files) {
		return options.resources(Arrays.asList(files)).map(this::reader).collect(Collectors.toList());
	}

	/**
	 * 
	 * @param file the file to create a reader for (can be CSV, Fixed-width, JSON,
	 *             XML)
	 * @return Reader for the given file
	 * @throws Exception
	 */
	public Iterator<Map<String, Object>> read(String file) throws Exception {
		return new ItemReaderIterator<>(reader(options.inputResource(file)));
	}

}
