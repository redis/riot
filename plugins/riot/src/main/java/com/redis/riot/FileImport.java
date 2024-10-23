package com.redis.riot;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;

import org.springframework.batch.core.Job;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.JsonLineMapper;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.item.file.separator.RecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.AbstractLineTokenizer;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FixedLengthTokenizer;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.batch.item.file.transform.RangeArrayPropertyEditor;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.Step;
import com.redis.riot.core.processor.RegexNamedGroupFunction;
import com.redis.riot.file.FileType;
import com.redis.riot.file.FileUtils;
import com.redis.riot.file.HeaderCallbackHandler;
import com.redis.riot.file.MapFieldSetMapper;
import com.redis.riot.file.MapToFieldFunction;
import com.redis.riot.file.ObjectMapperLineMapper;
import com.redis.riot.file.ToMapFunction;
import com.redis.riot.file.xml.XmlItemReader;
import com.redis.riot.file.xml.XmlItemReaderBuilder;
import com.redis.riot.file.xml.XmlObjectReader;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.KeyValue;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "file-import", description = "Import data from files.")
public class FileImport extends AbstractRedisImportCommand {

	@Parameters(arity = "1..*", description = "Files or URLs to import. Use '-' to read from stdin.", paramLabel = "FILE")
	private List<String> files;

	@Option(names = { "-t", "--filetype" }, description = "File type: ${COMPLETION-CANDIDATES}.", paramLabel = "<type>")
	private FileType fileType;

	@ArgGroup(exclusive = false)
	private FileReaderArgs fileReaderArgs = new FileReaderArgs();

	@Option(arity = "1..*", names = "--regex", description = "Regular expressions used to extract values from fields in the form field1=\"regex\" field2=\"regex\"...", paramLabel = "<f=rex>")
	private Map<String, Pattern> regexes = new LinkedHashMap<>();

	@Override
	protected Job job() throws IOException {
		Assert.notEmpty(files, "No file specified");
		List<Step<?, ?>> steps = new ArrayList<>();
		List<Resource> resources = new ArrayList<>();
		for (String file : files) {
			for (String expandedFile : FileUtils.expand(file)) {
				resources.add(fileReaderArgs.resource(expandedFile));
			}
		}
		for (Resource resource : resources) {
			Step<?, ?> step = step(resource);
			step.skip(ParseException.class);
			step.skip(org.springframework.batch.item.ParseException.class);
			step.noRetry(ParseException.class);
			step.noRetry(org.springframework.batch.item.ParseException.class);
			step.taskName(taskName(resource));
			steps.add(step);
		}
		return job(steps);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Step<?, ?> step(Resource resource) {
		String name = resource.getFilename();
		FileType type = fileType(resource);
		if (hasOperations()) {
			ItemReader<Map<String, Object>> reader = (ItemReader) createReader(resource, type, Map.class);
			RedisItemWriter<String, String, Map<String, Object>> writer = operationWriter();
			configureTargetRedisWriter(writer);
			return new Step<>(reader, writer).name(name).processor(processor());
		}
		Assert.isTrue(type != FileType.CSV, "CSV file import requires a Redis command");
		Assert.isTrue(type != FileType.FIXED, "Fixed-length file import requires a Redis command");
		ItemReader<KeyValue> reader = createReader(resource, type, KeyValue.class);
		RedisItemWriter<String, String, KeyValue<String>> writer = RedisItemWriter.struct();
		configureTargetRedisWriter(writer);
		return new Step<>(reader, writer).name(name);
	}

	@Override
	protected ItemProcessor<Map<String, Object>, Map<String, Object>> processor() {
		return RiotUtils.processor(super.processor(), regexProcessor());
	}

	private FileType fileType(Resource resource) {
		if (fileType == null) {
			FileType type = FileUtils.fileType(resource);
			Assert.notNull(type, String.format("Unknown type for file %s", resource.getFilename()));
			return type;
		}
		return fileType;
	}

	private String taskName(Resource resource) {
		return String.format("Importing %s", resource.getFilename());
	}

	private ItemProcessor<Map<String, Object>, Map<String, Object>> regexProcessor() {
		if (CollectionUtils.isEmpty(regexes)) {
			return null;
		}
		List<Function<Map<String, Object>, Map<String, Object>>> functions = new ArrayList<>();
		functions.add(Function.identity());
		regexes.entrySet().stream().map(e -> toFieldFunction(e.getKey(), e.getValue())).forEach(functions::add);
		return new FunctionItemProcessor<>(new ToMapFunction<>(functions));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Function<Map<String, Object>, Map<String, Object>> toFieldFunction(String key, Pattern regex) {
		return new MapToFieldFunction(key).andThen((Function) new RegexNamedGroupFunction(regex));
	}

	public static final String PIPE_DELIMITER = "|";

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public <T> ItemReader<T> createReader(Resource resource, FileType fileType, Class<T> itemType) {
		switch (fileType) {
		case XML:
			return xmlReader(resource, itemType);
		case JSON:
			return jsonReader(resource, itemType);
		case JSONL:
			return jsonlReader(resource, itemType);
		case CSV:
			return (FlatFileItemReader) flatFileReader(resource, delimitedLineTokenizer(delimiter(resource)));
		case FIXED:
			return (FlatFileItemReader) flatFileReader(resource, fixedLengthTokenizer());
		default:
			throw new UnsupportedOperationException("Unsupported file type: " + fileType);
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private <T> FlatFileItemReader<T> jsonlReader(Resource resource, Class<T> itemType) {
		if (Map.class.isAssignableFrom(itemType)) {
			FlatFileItemReaderBuilder<Map<String, Object>> reader = flatFileReader(resource);
			return (FlatFileItemReader) reader.fieldSetMapper(new MapFieldSetMapper()).lineMapper(new JsonLineMapper())
					.build();
		}
		FlatFileItemReaderBuilder<T> reader = flatFileReader(resource);
		reader.lineMapper(new ObjectMapperLineMapper<>(objectMapper(new ObjectMapper()), itemType));
		return reader.build();
	}

	private DelimitedLineTokenizer delimitedLineTokenizer(String delimiter) {
		DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
		tokenizer.setDelimiter(delimiter);
		tokenizer.setQuoteCharacter(fileReaderArgs.getFileArgs().getQuoteCharacter());
		if (!ObjectUtils.isEmpty(fileReaderArgs.getIncludedFields())) {
			tokenizer.setIncludedFields(
					fileReaderArgs.getIncludedFields().stream().mapToInt(Integer::intValue).toArray());
		}
		return tokenizer;
	}

	private FlatFileItemReader<Map<String, Object>> flatFileReader(Resource resource, AbstractLineTokenizer tokenizer) {
		if (ObjectUtils.isEmpty(fileReaderArgs.getFields())) {
			Assert.isTrue(fileReaderArgs.getFileArgs().isHeader(),
					String.format("Could not create reader for file '%s': no header or field names specified",
							resource.getFilename()));
		} else {
			tokenizer.setNames(fileReaderArgs.getFields().toArray(new String[0]));
		}
		FlatFileItemReaderBuilder<Map<String, Object>> builder = flatFileReader(resource);
		builder.fieldSetMapper(new MapFieldSetMapper());
		builder.lineTokenizer(tokenizer);
		builder.skippedLinesCallback(new HeaderCallbackHandler(tokenizer, headerIndex()));
		return builder.build();
	}

	private <T> FlatFileItemReaderBuilder<T> flatFileReader(Resource resource) {
		FlatFileItemReaderBuilder<T> builder = new FlatFileItemReaderBuilder<>();
		builder.resource(resource);
		builder.maxItemCount(fileReaderArgs.getMaxItemCount());
		if (fileReaderArgs.getFileArgs().getEncoding() != null) {
			builder.encoding(fileReaderArgs.getFileArgs().getEncoding());
		}
		builder.recordSeparatorPolicy(recordSeparatorPolicy());
		builder.linesToSkip(linesToSkip());
		builder.strict(true);
		builder.saveState(false);
		return builder;
	}

	private FixedLengthTokenizer fixedLengthTokenizer() {
		FixedLengthTokenizer tokenizer = new FixedLengthTokenizer();
		RangeArrayPropertyEditor editor = new RangeArrayPropertyEditor();
		Assert.notEmpty(fileReaderArgs.getColumnRanges(), "Column ranges are required");
		editor.setAsText(String.join(",", fileReaderArgs.getColumnRanges()));
		Range[] ranges = (Range[]) editor.getValue();
		Assert.notEmpty(ranges, "Invalid ranges specified: " + fileReaderArgs.getColumnRanges());
		tokenizer.setColumns(ranges);
		return tokenizer;
	}

	private <T extends ObjectMapper> T objectMapper(T objectMapper) {
		objectMapper.configure(DeserializationFeature.USE_LONG_FOR_INTS, true);
		SimpleModule module = new SimpleModule();
		module.addDeserializer(KeyValue.class, new KeyValueDeserializer());
		objectMapper.registerModule(module);
		return objectMapper;
	}

	private <T> XmlItemReader<T> xmlReader(Resource resource, Class<T> itemType) {
		XmlItemReaderBuilder<T> builder = new XmlItemReaderBuilder<>();
		builder.name(resource.getFilename() + "-xml-file-reader");
		builder.resource(resource);
		XmlObjectReader<T> objectReader = new XmlObjectReader<>(itemType);
		objectReader.setMapper(objectMapper(new XmlMapper()));
		builder.xmlObjectReader(objectReader);
		builder.maxItemCount(fileReaderArgs.getMaxItemCount());
		return builder.build();
	}

	private <T> JsonItemReader<T> jsonReader(Resource resource, Class<T> itemType) {
		JsonItemReaderBuilder<T> builder = new JsonItemReaderBuilder<>();
		builder.name(resource.getFilename() + "-json-file-reader");
		builder.resource(resource);
		JacksonJsonObjectReader<T> objectReader = new JacksonJsonObjectReader<>(itemType);
		objectReader.setMapper(objectMapper(new ObjectMapper()));
		builder.jsonObjectReader(objectReader);
		builder.maxItemCount(fileReaderArgs.getMaxItemCount());
		return builder.build();
	}

	private String delimiter(Resource resource) {
		if (fileReaderArgs.getFileArgs().getDelimiter() != null) {
			return fileReaderArgs.getFileArgs().getDelimiter();
		}
		String extension = FileUtils.fileExtension(resource);
		if (extension == null) {
			return DelimitedLineTokenizer.DELIMITER_COMMA;
		}
		switch (extension) {
		case FileUtils.PSV:
			return PIPE_DELIMITER;
		case FileUtils.TSV:
			return DelimitedLineTokenizer.DELIMITER_TAB;
		default:
			return DelimitedLineTokenizer.DELIMITER_COMMA;
		}
	}

	private RecordSeparatorPolicy recordSeparatorPolicy() {
		return new DefaultRecordSeparatorPolicy(String.valueOf(fileReaderArgs.getFileArgs().getQuoteCharacter()),
				fileReaderArgs.getContinuationString());
	}

	private int headerIndex() {
		if (fileReaderArgs.getHeaderLine() != null) {
			return fileReaderArgs.getHeaderLine();
		}
		return linesToSkip() - 1;
	}

	private int linesToSkip() {
		if (fileReaderArgs.getLinesToSkip() != null) {
			return fileReaderArgs.getLinesToSkip();
		}
		if (fileReaderArgs.getFileArgs().isHeader()) {
			return 1;
		}
		return 0;
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

	public FileReaderArgs getFileReaderArgs() {
		return fileReaderArgs;
	}

	public void setFileReaderArgs(FileReaderArgs args) {
		this.fileReaderArgs = args;
	}

	public Map<String, Pattern> getRegexes() {
		return regexes;
	}

	public void setRegexes(Map<String, Pattern> regexes) {
		this.regexes = regexes;
	}

	public FileType getFileType() {
		return fileType;
	}

	public void setFileType(FileType fileType) {
		this.fileType = fileType;
	}

}
