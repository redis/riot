package com.redislabs.riot.cli;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.redislabs.riot.generator.GeneratorReader;
import com.redislabs.riot.generator.GeneratorReaderBuilder;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Component
@Command(name = "gen", description = "Import randomly generated data")
public class GeneratorImportSubCommand extends ImportSubCommand {

	@Option(names = "--map", description = "SpEL expression to generate maps.", paramLabel = "<SpEL>")
	private String mapExpression;
	@Option(names = "--field" , description = "Field SpEL expressions.", paramLabel = "<name=SpEL>")
	private Map<String, String> fieldExpressions = new LinkedHashMap<>();
	@Option(names = "--locale", description = "Faker locale. (default: ${DEFAULT-VALUE}).")
	private Locale locale = Locale.ENGLISH;

	@Override
	public GeneratorReader reader() {
		GeneratorReaderBuilder builder = new GeneratorReaderBuilder();
		builder.setFields(fieldExpressions);
		builder.setLocale(locale);
		builder.setClient(getParent().getParent().redisConnectionBuilder().buildLettuceClient());
		return builder.build();
	}

	public String getSourceDescription() {
		String description = "generated";
		if (mapExpression != null) {
			description += " map " + mapExpression;
		}
		if (!fieldExpressions.isEmpty()) {
			description += " fields " + Arrays.toString(fieldExpressions.keySet().toArray());
		}
		return description;
	}

}
