package com.redis.riot.file;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.LineCallbackHandler;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.item.file.separator.RecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.AbstractLineTokenizer;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.item.file.transform.FixedLengthTokenizer;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.batch.item.file.transform.RangeArrayPropertyEditor;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.redis.riot.AbstractImportCommand;
import com.redis.riot.JobCommandContext;
import com.redis.riot.ProgressMonitor;
import com.redis.riot.file.FileImportOptions.FileType;
import com.redis.riot.file.resource.XmlItemReader;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "import", description = "Import CSV/JSON/XML files into Redis.")
public class FileImportCommand extends AbstractImportCommand {

	private static final Logger log = Logger.getLogger(FileImportCommand.class.getName());

	private static final String NAME = "file-import";
	private static final String DELIMITER_PIPE = "|";

	@Parameters(arity = "0..*", description = "One ore more files or URLs", paramLabel = "FILE")
	private List<String> files = new ArrayList<>();
	@ArgGroup(exclusive = false, heading = "Delimited and fixed-width file options%n")
	private FileImportOptions options = new FileImportOptions();

	public FileImportCommand() {
	}

	private FileImportCommand(Builder builder) {
		this.options = builder.options;
	}

	public List<String> getFiles() {
		return files;
	}

	public void setFiles(List<String> files) {
		this.files = files;
	}

	public FileImportOptions getOptions() {
		return options;
	}

	@Override
	protected Job job(JobCommandContext context) throws Exception {
		Assert.isTrue(!ObjectUtils.isEmpty(files), "No file specified");
		List<TaskletStep> steps = new ArrayList<>();
		for (String file : files) {
			for (Resource resource : resources(file)) {
				AbstractItemStreamItemReader<Map<String, Object>> reader = reader(resource);
				String name = resource.getDescription() + "-" + NAME;
				reader.setName(name);
				ProgressMonitor monitor = progressMonitor().task("Importing " + resource.getFilename()).build();
				steps.add(step(step(context, name, reader), monitor).skip(FlatFileParseException.class).build());
			}
		}
		Iterator<TaskletStep> iterator = steps.iterator();
		SimpleJobBuilder job = job(context, NAME, iterator.next());
		while (iterator.hasNext()) {
			job.next(iterator.next());
		}
		return job.build();
	}

	private List<Resource> resources(String file) throws IOException {
		List<Resource> resources = new ArrayList<>();
		for (String expandedFile : FileUtils.expand(file)) {
			resources.add(resource(expandedFile));
		}
		return resources;
	}

	private Resource resource(String file) throws IOException {
		return options.inputResource(file);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private AbstractItemStreamItemReader<Map<String, Object>> reader(Resource resource) {
		FileType type = options.getType().orElseGet(() -> type(resource));
		switch (type) {
		case DELIMITED:
			DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
			tokenizer.setDelimiter(options.getDelimiter().orElseGet(() -> delimiter(extension(resource))));
			tokenizer.setQuoteCharacter(options.getQuoteCharacter());
			if (!ObjectUtils.isEmpty(options.getIncludedFields())) {
				tokenizer.setIncludedFields(options.getIncludedFields());
			}
			log.log(Level.FINE, "Creating delimited reader with {0} for {1}", new Object[] { options, resource });
			return flatFileReader(resource, tokenizer);
		case FIXED:
			FixedLengthTokenizer fixedLengthTokenizer = new FixedLengthTokenizer();
			RangeArrayPropertyEditor editor = new RangeArrayPropertyEditor();
			Assert.notEmpty(options.getColumnRanges(), "Column ranges are required");
			editor.setAsText(String.join(",", options.getColumnRanges()));
			Range[] ranges = (Range[]) editor.getValue();
			if (ranges.length == 0) {
				throw new IllegalArgumentException("Invalid ranges specified: " + options.getColumnRanges());
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

	private FileType type(Resource resource) {
		FileExtension extension = extension(resource);
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

	private FileExtension extension(Resource resource) {
		return FileUtils.extension(resource)
				.orElseThrow(() -> new UnknownFileTypeException("Could not determine file type"));
	}

	private String delimiter(FileExtension extension) {
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
		if (!ObjectUtils.isEmpty(options.getNames())) {
			tokenizer.setNames(options.getNames().toArray(String[]::new));
		}
		FlatFileItemReaderBuilder<Map<String, Object>> builder = new FlatFileItemReaderBuilder<>();
		builder.resource(resource);
		builder.encoding(options.getEncoding().name());
		builder.lineTokenizer(tokenizer);
		builder.recordSeparatorPolicy(recordSeparatorPolicy());
		builder.linesToSkip(options.getLinesToSkip());
		builder.strict(true);
		builder.saveState(false);
		builder.fieldSetMapper(new MapFieldSetMapper());
		builder.skippedLinesCallback(new HeaderCallbackHandler(tokenizer));
		return builder.build();
	}

	private RecordSeparatorPolicy recordSeparatorPolicy() {
		return new DefaultRecordSeparatorPolicy(options.getQuoteCharacter().toString(),
				options.getContinuationString());
	}

	private static class MapFieldSetMapper implements FieldSetMapper<Map<String, Object>> {

		@Override
		public Map<String, Object> mapFieldSet(FieldSet fieldSet) {
			Map<String, Object> fields = new HashMap<>();
			String[] names = fieldSet.getNames();
			for (int index = 0; index < names.length; index++) {
				String name = names[index];
				String value = fieldSet.readString(index);
				if (value == null || value.length() == 0) {
					continue;
				}
				fields.put(name, value);
			}
			return fields;
		}
	}

	private static class HeaderCallbackHandler implements LineCallbackHandler {

		private final AbstractLineTokenizer tokenizer;

		public HeaderCallbackHandler(AbstractLineTokenizer tokenizer) {
			this.tokenizer = tokenizer;
		}

		@Override
		public void handleLine(String line) {
			log.log(Level.FINE, "Found header: {0}", line);
			FieldSet fieldSet = tokenizer.tokenize(line);
			List<String> fields = new ArrayList<>();
			for (int index = 0; index < fieldSet.getFieldCount(); index++) {
				fields.add(fieldSet.readString(index));
			}
			tokenizer.setNames(fields.toArray(new String[0]));
		}
	}

	public static Builder builder() {
		return new Builder();
	}

	public static class Builder {

		private FileImportOptions options = new FileImportOptions();

		public Builder options(FileImportOptions options) {
			Assert.notNull(options, "Options must not be null");
			this.options = options;
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
	public List<AbstractItemStreamItemReader<Map<String, Object>>> readers(String... files) throws IOException {
		List<AbstractItemStreamItemReader<Map<String, Object>>> readers = new ArrayList<>();
		for (String file : files) {
			for (String expandedFile : FileUtils.expand(file)) {
				readers.add(reader(resource(expandedFile)));
			}
		}
		return readers;
	}

	/**
	 * 
	 * @param file the file to create a reader for (can be CSV, Fixed-width, JSON,
	 *             XML)
	 * @return Reader for the given file
	 * @throws Exception
	 */
	public Iterator<Map<String, Object>> read(String file) throws Exception {
		return new ItemReaderIterator<>(reader(resource(file)));
	}

}
