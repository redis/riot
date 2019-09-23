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

	@Option(names = { "--proc-script" }, description = "Use an inline script to process items", paramLabel = "<script>")
	private String processorScript;
	@Option(names = { "--proc-file" }, description = "Use an external script to process items", paramLabel = "<file>")
	private File processorScriptFile;
	@Option(names = {
			"--proc-lang" }, description = "Script language (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
	private String processorScriptLanguage = "ECMAScript";
	@Option(arity = "1..*", names = {
			"--proc" }, description = "SpEL expression to process a field", paramLabel = "<name=exp>")
	private Map<String, String> fields;
	@Option(arity = "1..*", names = "--regex", description = "Extract fields from source field using regex", paramLabel = "<src=exp>")
	private Map<String, String> regexes;
	@Option(arity = "1..*", names = "--proc-var", description = "Register a variable in the processor context", paramLabel = "<v=exp>")
	private Map<String, String> variables = new LinkedHashMap<String, String>();
	@Option(names = "--proc-date", description = "Java date format (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
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
			processors.add(new SpelProcessor(redis(redis), new SimpleDateFormat(dateFormat), variables, fields));
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

	public Object redis(RedisConnectionOptions redis) {
		if (redis.isJedis()) {
			return redis.jedisPool();
		}
		if (redis.isCluster()) {
			return redis.lettuceClusterClient().connect().sync();
		}
		return redis.lettuceClient().connect().sync();
	}

}
