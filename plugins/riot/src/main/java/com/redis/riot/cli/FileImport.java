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
import java.util.stream.Stream;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineCallbackHandler;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.item.file.separator.RecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.AbstractLineTokenizer;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.item.file.transform.FixedLengthTokenizer;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.batch.item.file.transform.RangeArrayPropertyEditor;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.redis.riot.cli.common.AbstractOperationImportCommand;
import com.redis.riot.cli.common.CommandContext;
import com.redis.riot.cli.common.RiotStep;
import com.redis.riot.cli.file.FileOptions;
import com.redis.riot.cli.file.FlatFileOptions;
import com.redis.riot.core.FileExtension;
import com.redis.riot.core.FileType;
import com.redis.riot.core.FileUtils;
import com.redis.riot.core.ItemReaderIterator;
import com.redis.riot.core.MapFieldSetMapper;
import com.redis.riot.core.UnknownFileTypeException;
import com.redis.riot.core.resource.XmlItemReader;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "file-import", description = "Import from CSV/JSON/XML files.")
public class FileImport extends AbstractOperationImportCommand {

	private static final Logger log = Logger.getLogger(FileImport.class.getName());

	private static final String DELIMITER_PIPE = "|";

	@Parameters(arity = "0..*", description = "One ore more files or URLs", paramLabel = "FILE")
	protected List<String> files = new ArrayList<>();

	@Mixin
	private FileOptions fileOptions = new FileOptions();

	@ArgGroup(exclusive = false, heading = "Flat file options%n")
	private FlatFileOptions flatFileOptions = new FlatFileOptions();

	@Option(names = { "-t", "--filetype" }, description = "File type: ${COMPLETION-CANDIDATES}.", paramLabel = "<type>")
	private Optional<FileType> fileType = Optional.empty();

	public List<String> getFiles() {
		return files;
	}

	public void setFiles(List<String> files) {
		this.files = files;
	}

	public Optional<FileType> getFileType() {
		return fileType;
	}

	public void setFileType(FileType fileType) {
		this.fileType = Optional.of(fileType);
	}

	public FileOptions getFileOptions() {
		return fileOptions;
	}

	public void setFileOptions(FileOptions options) {
		this.fileOptions = options;
	}

	public FlatFileOptions getFlatFileOptions() {
		return flatFileOptions;
	}

	public void setFlatFileOptions(FlatFileOptions options) {
		this.flatFileOptions = options;
	}

	@Override
	protected Job job(CommandContext context) {
		Iterator<TaskletStep> steps = FileUtils.expandAll(files).map(fileOptions::inputResource)
				.map(r -> step(context, r)).map(RiotStep::build).map(SimpleStepBuilder::build).iterator();
		if (!steps.hasNext()) {
			throw new IllegalArgumentException("No files found");
		}
		SimpleJobBuilder job = job(commandName()).start(steps.next());
		while (steps.hasNext()) {
			job.next(steps.next());
		}
		return job.build();
	}

	private RiotStep<Map<String, Object>, Map<String, Object>> step(CommandContext context, Resource resource) {
		ItemReader<Map<String, Object>> reader = reader(resource);
		String name = commandName() + "-" + resource.getDescription();
		String task = "Importing " + resource.getFilename();
		return step(context, reader).name(name).task(task).skippableExceptions(ParseException.class);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private ItemReader<Map<String, Object>> reader(Resource resource) {
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
			log.log(Level.INFO, "Creating delimited reader for {0}", resource);
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
			log.log(Level.INFO, "Creating fixed-width reader for {0}", resource);
			return flatFileReader(resource, fixedLengthTokenizer);
		case XML:
			log.log(Level.INFO, "Creating XML reader for {0}", resource);
			return (XmlItemReader) FileUtils.xmlReader(resource, Map.class);
		case JSON:
			log.log(Level.INFO, "Creating JSON reader for {0}", resource);
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
		if (!ObjectUtils.isEmpty(flatFileOptions.getFields())) {
			tokenizer.setNames(flatFileOptions.getFields().toArray(new String[0]));
		}
		FlatFileItemReaderBuilder<Map<String, Object>> builder = new FlatFileItemReaderBuilder<>();
		builder.resource(resource);
		builder.encoding(fileOptions.getEncoding().name());
		builder.lineTokenizer(tokenizer);
		builder.recordSeparatorPolicy(recordSeparatorPolicy());
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

	private RecordSeparatorPolicy recordSeparatorPolicy() {
		return new DefaultRecordSeparatorPolicy(flatFileOptions.getQuoteCharacter().toString(),
				flatFileOptions.getContinuationString());
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
				log.log(Level.INFO, "Found header: {0}", line);
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

	/**
	 * 
	 * @param files Files to create readers for
	 * @return List of readers for the given files, which might contain wildcards
	 * @throws IOException
	 */
	public Stream<ItemReader<Map<String, Object>>> readers(String... files) {
		return FileUtils.expandAll(Arrays.asList(files)).map(fileOptions::inputResource).map(this::reader);
	}

	/**
	 * 
	 * @param file the file to create a reader for (can be CSV, Fixed-width, JSON,
	 *             XML)
	 * @return Reader for the given file
	 * @throws Exception
	 */
	public Iterator<Map<String, Object>> read(String file) throws Exception {
		return new ItemReaderIterator<>(reader(fileOptions.inputResource(file)));
	}

	@Override
	public String toString() {
		return "FileImport [files=" + files + ", fileOptions=" + fileOptions + ", flatFileOptions=" + flatFileOptions
				+ ", fileType=" + fileType + ", processorOptions=" + processorOptions + ", operationOptions="
				+ operationOptions + ", jobOptions=" + jobOptions + "]";
	}

}
