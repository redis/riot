package com.redislabs.riot;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import lombok.Getter;
import picocli.CommandLine.Option;

@Getter
public class MapProcessingOptions {

	@Option(arity = "1..*", names = "--spel", description = "SpEL expression to produce a field", paramLabel = "<f=exp>")
	private Map<String, String> spelFields = new HashMap<>();
	@Option(arity = "1..*", names = "--var", description = "Register a variable in the SpEL processor context", paramLabel = "<v=exp>")
	private Map<String, String> variables = new HashMap<>();
	@Option(names = "--date", description = "Processor date format (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
	private String dateFormat = new SimpleDateFormat().toPattern();
	@Option(arity = "1..*", names = "--regex", description = "Extract named values from source field using regex", paramLabel = "<f=exp>")
	private Map<String, String> regexes = new HashMap<>();

}
