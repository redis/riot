package com.redis.riot.file;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemReader;
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
import com.redis.riot.file.resource.XmlItemReader;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "import", description = "Import delimited, fixed-width, JSON, or XML files into Redis.")
public class FileImportCommand extends AbstractImportCommand {

	private enum FileType {
		DELIMITED, FIXED, JSON, XML
	}

	private static final Logger log = LoggerFactory.getLogger(FileImportCommand.class);

	private static final String NAME = "file-import";

	@CommandLine.Parameters(arity = "0..*", description = "One ore more files or URLs", paramLabel = "FILE")
	private List<String> files = new ArrayList<>();
	@CommandLine.Option(names = { "-t",
			"--filetype" }, description = "File type: ${COMPLETION-CANDIDATES}", paramLabel = "<type>")
	private Optional<FileType> type = Optional.empty();
	@CommandLine.ArgGroup(exclusive = false, heading = "Delimited and fixed-width file options%n")
	private FileImportOptions options = new FileImportOptions();

	public List<String> getFiles() {
		return files;
	}

	public void setFiles(List<String> files) {
		this.files = files;
	}

	public Optional<FileType> getType() {
		return type;
	}

	public void setType(FileType type) {
		this.type = Optional.of(type);
	}

	public FileImportOptions getOptions() {
		return options;
	}

	@Override
	protected Job job(JobBuilder jobBuilder) throws Exception {
		Iterator<TaskletStep> iterator = fileImportSteps().iterator();
		SimpleJobBuilder simpleJobBuilder = jobBuilder.start(iterator.next());
		while (iterator.hasNext()) {
			simpleJobBuilder.next(iterator.next());
		}
		return simpleJobBuilder.build();
	}

	private List<TaskletStep> fileImportSteps() throws Exception {
		Map<Resource, ItemReader<Map<String, Object>>> readers = readers();
		List<TaskletStep> steps = new ArrayList<>();
		for (Entry<Resource, ItemReader<Map<String, Object>>> entry : readers.entrySet()) {
			Resource resource = entry.getKey();
			ItemReader<Map<String, Object>> reader = entry.getValue();
			steps.add(step(resource.getDescription() + "-" + NAME, "Importing " + resource.getFilename(), reader)
					.skip(FlatFileParseException.class).build());
		}
		return steps;
	}

	public Map<Resource, ItemReader<Map<String, Object>>> readers() throws IOException {
		Assert.isTrue(!ObjectUtils.isEmpty(files), "No file specified");
		List<String> expandedFiles = FileUtils.expand(files);
		if (ObjectUtils.isEmpty(expandedFiles)) {
			throw new FileNotFoundException("File not found: " + String.join(", ", files));
		}
		Map<Resource, ItemReader<Map<String, Object>>> readers = new LinkedHashMap<>();
		for (String file : expandedFiles) {
			Resource resource = options.inputResource(file);
			AbstractItemStreamItemReader<Map<String, Object>> reader = reader(resource);
			reader.setName(file + "-" + NAME + "-reader");
			readers.put(resource, reader);
		}
		return readers;
	}

	private Optional<FileType> type(Optional<String> extension) {
		if (type.isPresent()) {
			return type;
		}
		if (extension.isEmpty()) {
			return Optional.empty();
		}
		switch (extension.get().toLowerCase()) {
		case FileUtils.EXTENSION_FW:
			return Optional.of(FileType.FIXED);
		case FileUtils.EXTENSION_JSON:
			return Optional.of(FileType.JSON);
		case FileUtils.EXTENSION_XML:
			return Optional.of(FileType.XML);
		case FileUtils.EXTENSION_CSV:
		case FileUtils.EXTENSION_PSV:
		case FileUtils.EXTENSION_TSV:
			return Optional.of(FileType.DELIMITED);
		default:
			return Optional.empty();
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public AbstractItemStreamItemReader<Map<String, Object>> reader(Resource resource) {
		Optional<String> extension = FileUtils.extension(resource.getFilename());
		Optional<FileType> fileType = type(extension);
		if (fileType.isEmpty()) {
			throw new IllegalArgumentException("Could not determine file type for " + resource);
		}
		switch (fileType.get()) {
		case DELIMITED:
			DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
			tokenizer.setDelimiter(options.delimiter(extension));
			tokenizer.setQuoteCharacter(options.getQuoteCharacter());
			if (!ObjectUtils.isEmpty(options.getIncludedFields())) {
				tokenizer.setIncludedFields(options.getIncludedFields());
			}
			log.debug("Creating delimited reader with {} for {}", options, resource);
			return flatFileReader(resource, tokenizer);
		case FIXED:
			FixedLengthTokenizer fixedLengthTokenizer = new FixedLengthTokenizer();
			RangeArrayPropertyEditor editor = new RangeArrayPropertyEditor();
			Assert.notEmpty(options.getColumnRanges(), "Column ranges are required");
			editor.setAsText(String.join(",", options.getColumnRanges()));
			Range[] ranges = (Range[]) editor.getValue();
			if (ranges.length == 0) {
				throw new IllegalArgumentException(
						"Invalid ranges specified: " + Arrays.toString(options.getColumnRanges()));
			}
			fixedLengthTokenizer.setColumns(ranges);
			log.debug("Creating fixed-width reader with {} for {}", options, resource);
			return flatFileReader(resource, fixedLengthTokenizer);
		case XML:
			log.debug("Creating XML reader for {}", resource);
			return (XmlItemReader) FileUtils.xmlReader(resource, Map.class);
		default:
			log.debug("Creating JSON reader for {}", resource);
			return (JsonItemReader) FileUtils.jsonReader(resource, Map.class);
		}
	}

	private FlatFileItemReader<Map<String, Object>> flatFileReader(Resource resource, AbstractLineTokenizer tokenizer) {
		if (!ObjectUtils.isEmpty(options.getNames())) {
			tokenizer.setNames(options.getNames());
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
			log.debug("Found header {}", line);
			FieldSet fieldSet = tokenizer.tokenize(line);
			List<String> fields = new ArrayList<>();
			for (int index = 0; index < fieldSet.getFieldCount(); index++) {
				fields.add(fieldSet.readString(index));
			}
			tokenizer.setNames(fields.toArray(new String[0]));
		}
	}

}
