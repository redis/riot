package com.redislabs.riot.cli;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.batch.item.support.ScriptItemProcessor;
import org.springframework.batch.item.support.builder.ScriptItemProcessorBuilder;
import org.springframework.core.io.FileSystemResource;

import com.redislabs.riot.processor.RegexProcessor;
import com.redislabs.riot.processor.SpelProcessor;

import picocli.CommandLine.Option;

public class MapProcessorOptions {

	@Option(arity = "1..*", names = "--regex", description = "Extract named values from source field using regex", paramLabel = "<src=exp>")
	private Map<String, String> regexes;
	@Option(arity = "1..*", names = "--spel", description = "SpEL expression to process a field", paramLabel = "<name=exp>")
	private Map<String, String> spel;
	@Option(arity = "1..*", names = "--spel-var", description = "Register a variable in the SpEL processor context", paramLabel = "<v=exp>")
	private Map<String, String> variables;
	@Option(names = "--date-format", description = "Processor date format (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
	private String dateFormat = new SimpleDateFormat().toPattern();
	@Option(names = "--script", description = "Use an inline script to process items", paramLabel = "<script>")
	private String script;
	@Option(names = "--script-file", description = "Use an external script to process items", paramLabel = "<file>")
	private File scriptFile;
	@Option(names = "--script-lang", description = "Script language (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
	private String processorScriptLanguage = "ECMAScript";

	public ScriptItemProcessor<Map<String, Object>, Map<String, Object>> scriptProcessor() throws Exception {
		if (script == null && scriptFile == null) {
			return null;
		}
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
		return processor;
	}

	public SpelProcessor spelProcessor() {
		if (spel == null) {
			return null;
		}
		return new SpelProcessor(new SimpleDateFormat(dateFormat), variables, spel);
	}

	public void addField(String name, String expression) {
		spel.put(name, expression);
	}

	private RegexProcessor regexProcessor() {
		if (regexes == null) {
			return null;
		}
		return new RegexProcessor(regexes);
	}

	@SuppressWarnings("unchecked")
	public ItemProcessor<Map<String, Object>, Map<String, Object>> processor() throws Exception {
		return (ItemProcessor<Map<String, Object>, Map<String, Object>>) processors(regexProcessor(), spelProcessor(),
				scriptProcessor());

	}

	private ItemProcessor<?, ?> processors(ItemProcessor<?, ?>... processors) {
		List<ItemProcessor<?, ?>> processorList = new ArrayList<>();
		for (ItemProcessor<?, ?> processor : processors) {
			if (processor == null) {
				continue;
			}
			processorList.add(processor);
		}
		if (processorList.isEmpty()) {
			return null;
		}
		if (processorList.size() == 1) {
			return processorList.get(0);
		}
		CompositeItemProcessor<?, ?> composite = new CompositeItemProcessor<>();
		composite.setDelegates(processorList);
		return composite;

	}

}
