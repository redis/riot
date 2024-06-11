package com.redis.riot;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.springframework.batch.core.Job;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import com.redis.riot.core.RiotException;
import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.Step;
import com.redis.riot.core.function.RegexNamedGroupFunction;
import com.redis.riot.file.FileReaderArgs;
import com.redis.riot.file.FileReaderFactory;
import com.redis.riot.file.MapToFieldFunction;
import com.redis.riot.file.ToMapFunction;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.KeyValue;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "file-import", description = "Import data from files.")
public class FileImport extends AbstractImportCommand {

	private final FileReaderFactory factory = new FileReaderFactory();

	@ArgGroup(exclusive = false)
	private FileReaderArgs fileReaderArgs = new FileReaderArgs();

	@Option(arity = "1..*", names = "--regex", description = "Regular expressions used to extract values from fields in the form field1=\"regex\" field2=\"regex\"...", paramLabel = "<f=rex>")
	private Map<String, Pattern> regexes = new LinkedHashMap<>();

	@Override
	public void afterPropertiesSet() throws Exception {
		factory.addDeserializer(KeyValue.class, new KeyValueDeserializer());
		factory.setArgs(fileReaderArgs);
		factory.setItemType(itemType());
		super.afterPropertiesSet();
	}

	@Override
	protected Job job() {
		Assert.notEmpty(fileReaderArgs.getFiles(), "No file specified");
		return job(fileReaderArgs.resources().stream().map(this::step).collect(Collectors.toList()));
	}

	@SuppressWarnings("unchecked")
	private Step<?, ?> step(Resource resource) {
		ItemReader<?> reader;
		try {
			reader = factory.create(resource);
		} catch (Exception e) {
			throw new RiotException("Could not create reader for file " + resource, e);
		}
		Step<?, ?> step = new Step<>(reader, writer());
		step.skip(ParseException.class);
		step.skip(org.springframework.batch.item.ParseException.class);
		step.noRetry(ParseException.class);
		step.noRetry(org.springframework.batch.item.ParseException.class);
		step.processor(processor());
		step.name(resource.getFilename());
		step.taskName(taskName(resource));
		return step;
	}

	private String taskName(Resource resource) {
		return String.format("Importing %s", resource.getFilename());
	}

	@Override
	protected ItemProcessor<Map<String, Object>, Map<String, Object>> mapProcessor() {
		return RiotUtils.processor(super.mapProcessor(), regexProcessor());

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

	@SuppressWarnings("rawtypes")
	private ItemWriter writer() {
		if (hasOperations()) {
			return mapWriter();
		}
		RedisItemWriter<String, String, KeyValue<String, Object>> structWriter = RedisItemWriter.struct();
		configure(structWriter);
		return structWriter;
	}

	private Class<?> itemType() {
		if (hasOperations()) {
			return Map.class;
		}
		return KeyValue.class;
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

}
