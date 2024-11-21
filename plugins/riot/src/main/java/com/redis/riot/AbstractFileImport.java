package com.redis.riot;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.springframework.batch.core.Job;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.MimeType;

import com.redis.riot.core.RiotInitializationException;
import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.Step;
import com.redis.riot.core.processor.RegexNamedGroupFunction;
import com.redis.riot.file.FileReaderRegistry;
import com.redis.riot.file.FileReaderResult;
import com.redis.riot.file.ReadOptions;
import com.redis.riot.file.StdInProtocolResolver;
import com.redis.riot.function.MapToFieldFunction;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.KeyValue;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "file-import", description = "Import data from files.")
public abstract class AbstractFileImport extends AbstractRedisImportCommand {

	public static final String STDIN_FILENAME = "-";

	@Parameters(arity = "1..*", description = "Files or URLs to import. Use '-' to read from stdin.", paramLabel = "FILE")
	private List<String> files;

	@ArgGroup(exclusive = false)
	private FileReaderArgs fileReaderArgs = new FileReaderArgs();

	@Option(arity = "1..*", names = "--regex", description = "Regular expressions used to extract values from fields in the form field1=\"regex\" field2=\"regex\"...", paramLabel = "<f=rex>")
	private Map<String, Pattern> regexes = new LinkedHashMap<>();

	private FileReaderRegistry readerRegistry;
	private ReadOptions readOptions;

	@Override
	protected void initialize() throws RiotInitializationException {
		super.initialize();
		Assert.notEmpty(files, "No file specified");
		readerRegistry = readerRegistry();
		readOptions = readOptions();
	}

	private FileReaderRegistry readerRegistry() {
		FileReaderRegistry registry = FileReaderRegistry.defaultReaderRegistry();
		StdInProtocolResolver stdInProtocolResolver = new StdInProtocolResolver();
		stdInProtocolResolver.setFilename(STDIN_FILENAME);
		registry.addProtocolResolver(stdInProtocolResolver);
		return registry;
	}

	@Override
	protected Job job() {
		return job(files.stream().map(this::step).collect(Collectors.toList()));
	}

	private Step<?, ?> step(String location) {
		FileReaderResult reader = readerRegistry.find(location, readOptions);
		Assert.notNull(reader.getReader(),
				() -> String.format("No reader found for type %s and file %s", reader.getType(), location));
		RedisItemWriter<?, ?, ?> writer = writer();
		configureTargetRedisWriter(writer);
		Step<?, ?> step = new Step<>(reader.getReader(), writer);
		step.name(location);
		if (hasOperations()) {
			step.processor(RiotUtils.processor(processor(), regexProcessor()));
		}
		step.skip(ParseException.class);
		step.skip(org.springframework.batch.item.ParseException.class);
		step.noRetry(ParseException.class);
		step.noRetry(org.springframework.batch.item.ParseException.class);
		step.taskName(String.format("Importing %s", reader.getResource().getFilename()));
		return step;
	}

	private ReadOptions readOptions() {
		ReadOptions options = fileReaderArgs.readOptions();
		options.setContentType(getFileType());
		options.setItemType(itemType());
		options.addDeserializer(KeyValue.class, new KeyValueDeserializer());
		return options;
	}

	private Class<?> itemType() {
		if (hasOperations()) {
			return Map.class;
		}
		return KeyValue.class;
	}

	private RedisItemWriter<?, ?, ?> writer() {
		if (hasOperations()) {
			return operationWriter();
		}
		return RedisItemWriter.struct();
	}

	protected abstract MimeType getFileType();

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

	public FileReaderRegistry getReaderRegistry() {
		return readerRegistry;
	}

	public void setReaderRegistry(FileReaderRegistry registry) {
		this.readerRegistry = registry;
	}

}
