package com.redis.riot;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.springframework.batch.core.Job;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.MimeType;

import com.redis.riot.core.RiotException;
import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.Step;
import com.redis.riot.core.processor.RegexNamedGroupFunction;
import com.redis.riot.file.FileReaderRegistry;
import com.redis.riot.file.ReadOptions;
import com.redis.riot.file.ReaderFactory;
import com.redis.riot.file.ResourceFactory;
import com.redis.riot.file.ResourceMap;
import com.redis.riot.file.RiotResourceMap;
import com.redis.riot.file.StdInProtocolResolver;
import com.redis.riot.function.MapToFieldFunction;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.KeyValue;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

public abstract class AbstractFileImport extends AbstractRedisImportCommand {

	public static final String STDIN_FILENAME = "-";
	private static final Set<MimeType> keyValueTypes = new HashSet<>(
			Arrays.asList(ResourceMap.JSON, ResourceMap.JSON_LINES, ResourceMap.XML));

	@Parameters(arity = "1..*", description = "Files or URLs to import. Use '-' to read from stdin.", paramLabel = "FILE")
	private List<String> files;

	@ArgGroup(exclusive = false)
	private FileReaderArgs fileReaderArgs = new FileReaderArgs();

	@Option(arity = "1..*", names = "--regex", description = "Regular expressions used to extract values from fields in the form field1=\"regex\" field2=\"regex\"...", paramLabel = "<f=rex>")
	private Map<String, Pattern> regexes = new LinkedHashMap<>();

	private FileReaderRegistry readerRegistry;
	private RiotResourceMap resourceMap;
	private ResourceFactory resourceFactory;
	private ReadOptions readOptions;

	@Override
	protected void initialize() {
		super.initialize();
		Assert.notEmpty(files, "No file specified");
		readerRegistry = readerRegistry();
		resourceFactory = resourceFactory();
		resourceMap = resourceMap();
		readOptions = readOptions();
	}

	protected RiotResourceMap resourceMap() {
		return RiotResourceMap.defaultResourceMap();
	}

	protected FileReaderRegistry readerRegistry() {
		return FileReaderRegistry.defaultReaderRegistry();
	}

	protected ResourceFactory resourceFactory() {
		ResourceFactory resourceFactory = new ResourceFactory();
		StdInProtocolResolver stdInProtocolResolver = new StdInProtocolResolver();
		stdInProtocolResolver.setFilename(STDIN_FILENAME);
		resourceFactory.addProtocolResolver(stdInProtocolResolver);
		return resourceFactory;
	}

	@Override
	protected Job job() {
		return job(files.stream().map(this::step).collect(Collectors.toList()));
	}

	private Step<?, ?> step(String location) {
		Resource resource;
		try {
			resource = resourceFactory.resource(location, readOptions);
		} catch (IOException e) {
			throw new RiotException(String.format("Could not create resource from %s", location), e);
		}
		MimeType type = readOptions.getContentType() == null ? resourceMap.getContentTypeFor(resource)
				: readOptions.getContentType();
		ReaderFactory readerFactory = readerRegistry.getReaderFactory(type);
		Assert.notNull(readerFactory, () -> String.format("No reader found for file %s", location));
		ItemReader<?> reader = readerFactory.create(resource, readOptions);
		RedisItemWriter<?, ?, ?> writer = writer();
		configureTargetRedisWriter(writer);
		Step<?, ?> step = new Step<>(reader, writer);
		step.name(location);
		if (hasOperations()) {
			step.processor(RiotUtils.processor(processor(), regexProcessor()));
		} else {
			Assert.isTrue(keyValueTypes.contains(type), "No Redis operation specified");
		}
		step.skip(ParseException.class);
		step.skip(org.springframework.batch.item.ParseException.class);
		step.noRetry(ParseException.class);
		step.noRetry(org.springframework.batch.item.ParseException.class);
		step.taskName(String.format("Importing %s", resource.getFilename()));
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
