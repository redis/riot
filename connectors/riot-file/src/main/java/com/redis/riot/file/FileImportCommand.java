package com.redis.riot.file;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
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
	private static final String DELIMITER_PIPE = "|";

	@CommandLine.Parameters(arity = "0..*", description = "One ore more files or URLs", paramLabel = "FILE")
	private List<String> files = new ArrayList<>();
	@CommandLine.Option(names = { "-t",
			"--filetype" }, description = "File type: ${COMPLETION-CANDIDATES}", paramLabel = "<type>")
	private FileType type;
	@CommandLine.ArgGroup(exclusive = false, heading = "Delimited and fixed-width file options%n")
	private FileImportOptions options = new FileImportOptions();

	public List<String> getFiles() {
		return files;
	}

	public void setFiles(List<String> files) {
		this.files = files;
	}

	public FileType getType() {
		return type;
	}

	public void setType(FileType type) {
		this.type = type;
	}

	public FileImportOptions getOptions() {
		return options;
	}

	@Override
	protected Flow flow() throws Exception {
		Assert.isTrue(!ObjectUtils.isEmpty(files), "No file specified");
		List<String> expandedFiles = FileUtils.expand(files);
		if (ObjectUtils.isEmpty(expandedFiles)) {
			throw new FileNotFoundException("File not found: " + String.join(", ", files));
		}
		List<Step> steps = new ArrayList<>();
		for (String file : expandedFiles) {
			FileType fileType = type(file);
			if (fileType == null) {
				throw new IllegalArgumentException("Could not determine type of file " + file);
			}
			Resource resource = options.inputResource(file);
			AbstractItemStreamItemReader<Map<String, Object>> reader = reader(file, fileType, resource);
			reader.setName(file + "-" + NAME + "-reader");
			FaultTolerantStepBuilder<Map<String, Object>, Map<String, Object>> step = step(file + "-" + NAME,
					"Importing " + resource.getFilename(), reader);
			step.skip(FlatFileParseException.class);
			steps.add(step.build());
		}
		return flow(NAME, steps.toArray(new Step[0]));
	}

	private FileType type(String file) {
		if (type == null) {
			String extension = FileUtils.extension(file);
			if (extension != null) {
				switch (extension.toLowerCase()) {
				case FileUtils.EXTENSION_FW:
					return FileType.FIXED;
				case FileUtils.EXTENSION_JSON:
					return FileType.JSON;
				case FileUtils.EXTENSION_XML:
					return FileType.XML;
				case FileUtils.EXTENSION_CSV:
				case FileUtils.EXTENSION_PSV:
				case FileUtils.EXTENSION_TSV:
					return FileType.DELIMITED;
				default:
					return null;
				}
			}
			return null;
		}
		return type;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private AbstractItemStreamItemReader<Map<String, Object>> reader(String file, FileType fileType,
			Resource resource) {
		switch (fileType) {
		case DELIMITED:
			DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
			tokenizer.setDelimiter(delimiter(file));
			tokenizer.setQuoteCharacter(options.getQuoteCharacter());
			if (!ObjectUtils.isEmpty(options.getIncludedFields())) {
				tokenizer.setIncludedFields(options.getIncludedFields());
			}
			log.debug("Creating delimited reader with {} for file {}", options, file);
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
			log.debug("Creating fixed-width reader with {} for file {}", options, file);
			return flatFileReader(resource, fixedLengthTokenizer);
		case XML:
			log.debug("Creating XML reader for file {}", file);
			return (XmlItemReader) FileUtils.xmlReader(resource, Map.class);
		default:
			log.debug("Creating JSON reader for file {}", file);
			return (JsonItemReader) FileUtils.jsonReader(resource, Map.class);
		}
	}

	private String delimiter(String file) {
		if (options.getDelimiter() == null) {
			String extension = FileUtils.extension(file);
			if (extension == null) {
				throw new IllegalArgumentException("Could not determine delimiter for extension " + extension);
			}
			switch (extension.toLowerCase()) {
			case FileUtils.EXTENSION_CSV:
				return DelimitedLineTokenizer.DELIMITER_COMMA;
			case FileUtils.EXTENSION_PSV:
				return DELIMITER_PIPE;
			case FileUtils.EXTENSION_TSV:
				return DelimitedLineTokenizer.DELIMITER_TAB;
			default:
				throw new IllegalArgumentException("Unknown extension: " + extension);
			}
		}
		return options.getDelimiter();
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
		builder.linesToSkip(linesToSkip());
		builder.strict(true);
		builder.saveState(false);
		builder.fieldSetMapper(new MapFieldSetMapper());
		builder.skippedLinesCallback(new HeaderCallbackHandler(tokenizer));
		return builder.build();
	}

	private int linesToSkip() {
		if (options.getLinesToSkip() == null) {
			if (options.isHeader()) {
				return 1;
			}
			return 0;
		}
		return options.getLinesToSkip();
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
