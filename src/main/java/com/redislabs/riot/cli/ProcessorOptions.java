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

import com.redislabs.riot.batch.processor.RegexProcessor;
import com.redislabs.riot.batch.processor.SpelProcessor;

import lombok.Data;
import lombok.experimental.Accessors;
import picocli.CommandLine.Option;

@Accessors(fluent = true)
public @Data class ProcessorOptions {

	@Option(names = { "--script" }, description = "Use an inline script to process items", paramLabel = "<script>")
	private String script;
	@Option(names = { "--script-file" }, description = "Use an external script to process items", paramLabel = "<file>")
	private File scriptFile;
	@Option(names = {
			"--language" }, description = "Script language (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
	private String processorScriptLanguage = "ECMAScript";
	@Option(arity = "1..*", names = {
			"--proc" }, description = "SpEL expression to process a field", paramLabel = "<name=exp>")
	private Map<String, String> fields = new LinkedHashMap<>();
	@Option(arity = "1..*", names = "--regex", description = "Extract fields from source field using regex", paramLabel = "<src=exp>")
	private Map<String, String> regexes = new LinkedHashMap<>();
	@Option(arity = "1..*", names = "--var", description = "Register a variable in the processor context", paramLabel = "<v=exp>")
	private Map<String, String> variables = new LinkedHashMap<String, String>();
	@Option(names = "--date-format", description = "Java date format (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
	private String dateFormat = new SimpleDateFormat().toPattern();

	public void addField(String name, String expression) {
		fields.put(name, expression);
	}

	public ItemProcessor<Map<String, Object>, Map<String, Object>> processor(
			ItemProcessor<Map<String, Object>, Map<String, Object>> post) throws Exception {
		List<ItemProcessor<Map<String, Object>, Map<String, Object>>> processors = new ArrayList<>();
		if (!regexes.isEmpty()) {
			processors.add(new RegexProcessor(regexes));
		}
		if (script != null || scriptFile != null) {
			System.setProperty("nashorn.args", "--no-deprecation-warning");
			ScriptItemProcessorBuilder<Map<String, Object>, Map<String, Object>> builder = new ScriptItemProcessorBuilder<>();
			builder.language(processorScriptLanguage);
			if (script != null) {
				builder.scriptSource(script);
			}
			if (scriptFile != null) {
				builder.scriptResource(new FileSystemResource(scriptFile));
			}
			ScriptItemProcessor<Map<String, Object>, Map<String, Object>> processor = builder.build();
			processor.afterPropertiesSet();
			processors.add(processor);
		}
		if (!fields.isEmpty()) {
			processors.add(new SpelProcessor(new SimpleDateFormat(dateFormat), variables, fields));
		}
		if (post != null) {
			processors.add(post);
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
