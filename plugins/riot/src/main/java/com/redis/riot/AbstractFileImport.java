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
import java.util.stream.Collectors;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.step.builder.StepBuilderException;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.MimeType;

import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.Step;
import com.redis.riot.core.processor.RegexNamedGroupFunction;
import com.redis.riot.file.ReadOptions;
import com.redis.riot.file.FileReaderRegistry;
import com.redis.riot.function.MapToFieldFunction;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.KeyValue;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "file-import", description = "Import data from files.")
public abstract class AbstractFileImport extends AbstractRedisImportCommand {

	private FileReaderRegistry readerRegistry = FileReaderRegistry.defaultReaderRegistry();

	@Parameters(arity = "1..*", description = "Files or URLs to import. Use '-' to read from stdin.", paramLabel = "FILE")
	private List<String> files;

	@ArgGroup(exclusive = false)
	private FileReaderArgs fileReaderArgs = new FileReaderArgs();

	@Option(arity = "1..*", names = "--regex", description = "Regular expressions used to extract values from fields in the form field1=\"regex\" field2=\"regex\"...", paramLabel = "<f=rex>")
	private Map<String, Pattern> regexes = new LinkedHashMap<>();

	@Override
	protected Job job() {
		Assert.notEmpty(files, "No file specified");
		ReadOptions options = readOptions();
		return job(files.stream().map(f -> step(f, options)).collect(Collectors.toList()));
	}

	private Step<?, ?> step(String location, ReadOptions options) {
		ItemReader<?> reader;
		try {
			reader = readerRegistry.get(location, options);
		} catch (IOException e) {
			throw new StepBuilderException(e);
		}
		RedisItemWriter<?, ?, ?> writer = writer();
		configureTargetRedisWriter(writer);
		Step<?, ?> step = new Step<>(reader, writer);
		step.name(location);
		if (hasOperations()) {
			step.processor(RiotUtils.processor(processor(), regexProcessor()));
		}
		step.skip(ParseException.class);
		step.skip(org.springframework.batch.item.ParseException.class);
		step.noRetry(ParseException.class);
		step.noRetry(org.springframework.batch.item.ParseException.class);
		step.taskName(String.format("Importing %s", location));
		return step;
	}

	private ReadOptions readOptions() {
		ReadOptions options = fileReaderArgs.readOptions();
		options.setType(getFileType());
		options.setItemType(itemType());
		options.addDeserializer(KeyValue.class, new KeyValueDeserializer());
		return options;
	}

	private Class<?> itemType() {
		return hasOperations() ? Map.class : KeyValue.class;
	}

	private RedisItemWriter<?, ?, ?> writer() {
		return hasOperations() ? operationWriter() : RedisItemWriter.struct();
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

	public void setReaderRegistry(FileReaderRegistry registry) {
		this.readerRegistry = registry;
	}

}
