package com.redislabs.riot.cli;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.redislabs.riot.generator.GeneratorReader;
import com.redislabs.riot.generator.GeneratorReaderBuilder;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Component
@Command(name = "gen", description = "Import randomly generated data", sortOptions = false)
public class GeneratorImportSubCommand extends AbstractImportSubCommand {

	@Option(names = "--map", description = "SpEL expression to generate maps.", order = 3, paramLabel = "<SpEL>")
	private String mapExpression;
	@Option(names = { "-f", "--field" }, description = "Field SpEL expressions.", order = 3, paramLabel = "<name=SpEL>")
	private Map<String, String> fieldExpressions = new LinkedHashMap<>();
	@Option(names = "--locale", description = "Faker locale. (default: ${DEFAULT-VALUE}).", order = 3)
	private String locale = "en-US";

	@Override
	public GeneratorReader reader() {
		GeneratorReaderBuilder builder = new GeneratorReaderBuilder();
		builder.setMapExpression(mapExpression);
		builder.setFieldExpressions(fieldExpressions);
		builder.setLocale(locale);
		builder.setConnection(getParent().redisConnectionBuilder().buildClient().connect());
		return builder.build();
	}

	@Override
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
