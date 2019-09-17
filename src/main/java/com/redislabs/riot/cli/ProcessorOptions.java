package com.redislabs.riot.cli;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.batch.item.support.ScriptItemProcessor;
import org.springframework.batch.item.support.builder.ScriptItemProcessorBuilder;
import org.springframework.core.io.FileSystemResource;

import com.redislabs.riot.batch.RegexProcessor;
import com.redislabs.riot.batch.SpelProcessor;
import com.redislabs.riot.cli.redis.RedisConnectionOptions;

import picocli.CommandLine.Option;

public class ProcessorOptions {

	@Option(names = { "--processor-script" }, description = "Use an inline script to process items")
	private String processorScript;
	@Option(names = { "--processor-script-file" }, description = "Use an external script to process items")
	private File processorScriptFile;
	@Option(names = {
			"--processor-script-language" }, description = "Language for the inline script (default: ${DEFAULT-VALUE})", paramLabel = "<lang>")
	private String processorScriptLanguage = "ECMAScript";
	@Option(arity = "1..*", names = {
			"--processor" }, description = "SpEL expression to process a field", paramLabel = "<name=expression>")
	private Map<String, String> fields;
	@Option(arity = "1..*", names = { "-r",
			"--regex" }, description = "Extract fields from a source field using a regular expression", paramLabel = "<source=regex>")
	private Map<String, String> regexes;
	@Option(arity = "1..*", names = "--processor-variable", description = "Register a variable in the processor context", paramLabel = "<name=expression>")
	private Map<String, String> variables = new LinkedHashMap<String, String>();
	@Option(names = "--processor-date-format", description = "java.text.SimpleDateFormat pattern for 'date' processor variable (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
	private String dateFormat = new SimpleDateFormat().toPattern();

	public ItemProcessor<Map<String, Object>, Map<String, Object>> processor(RedisConnectionOptions redis) throws Exception {
		List<ItemProcessor<Map<String, Object>, Map<String, Object>>> processors = new ArrayList<>();
		if (regexes != null) {
			processors.add(new RegexProcessor(regexes));
		}
		if (processorScript != null || processorScriptFile != null) {
			System.setProperty("nashorn.args", "--no-deprecation-warning");
			ScriptItemProcessorBuilder<Map<String, Object>, Map<String, Object>> builder = new ScriptItemProcessorBuilder<>();
			builder.language(processorScriptLanguage);
			if (processorScript != null) {
				builder.scriptSource(processorScript);
			}
			if (processorScriptFile != null) {
				builder.scriptResource(new FileSystemResource(processorScriptFile));
			}
			ScriptItemProcessor<Map<String, Object>, Map<String, Object>> processor = builder.build();
			processor.afterPropertiesSet();
			processors.add(processor);
		}
		if (fields != null) {
			processors.add(new SpelProcessor(redis.redis(), new SimpleDateFormat(dateFormat), variables, fields));
		}
		if (processors.isEmpty()) {
			return null;
		}
		if (processors.size() == 1) {
			return processors.get(0);
		}
		CompositeItemProcessor<Map<String, Object>, Map<String, Object>> processor = new CompositeItemProcessor<>();
		processor.setDelegates(processors);
		return processor;
	}

}
