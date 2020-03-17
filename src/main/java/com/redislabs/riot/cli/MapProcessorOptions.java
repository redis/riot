package com.redislabs.riot.cli;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.support.CompositeItemProcessor;

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

	public ItemProcessor<Map<String, Object>, Map<String, Object>> processor() {
		if (regexes == null) {
			if (spel == null) {
				return null;
			}
			return spelProcessor();
		}
		if (spel == null) {
			return regexProcessor();
		}
		CompositeItemProcessor<Map<String, Object>, Map<String, Object>> processor = new CompositeItemProcessor<>();
		processor.setDelegates(Arrays.asList(regexProcessor(), spelProcessor()));
		return processor;
	}

	private RegexProcessor regexProcessor() {
		return new RegexProcessor(regexes);
	}

	private SpelProcessor spelProcessor() {
		return new SpelProcessor(new SimpleDateFormat(dateFormat), variables, spel);
	}

	public void addField(String name, String expression) {
		spel.put(name, expression);
	}

}
