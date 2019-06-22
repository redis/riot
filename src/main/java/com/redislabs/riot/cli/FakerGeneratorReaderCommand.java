package com.redislabs.riot.cli;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import com.redislabs.riot.generator.GeneratorReader;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "faker", description = "Faker-generated data")
public class FakerGeneratorReaderCommand extends AbstractReaderCommand {

	@Option(names = "--field", required = true, description = "Field SpEL expressions", paramLabel = "<name=SpEL>")
	private Map<String, String> fieldExpressions = new LinkedHashMap<>();
	@Option(names = "--locale", description = "Faker locale")
	private Locale locale = Locale.ENGLISH;

	@Override
	public GeneratorReader reader() {
		SpelExpressionParser parser = new SpelExpressionParser();
		Map<String, Expression> fieldExpressionMap = new LinkedHashMap<String, Expression>();
		fieldExpressions.forEach((k, v) -> fieldExpressionMap.put(k, parser.parseExpression(v)));
		GeneratorReader reader = new GeneratorReader();
		reader.setFieldExpressions(fieldExpressionMap);
		reader.setLocale(locale);
		return reader;
	}

	@Override
	public String getSourceDescription() {
		String description = "generated";
		if (!fieldExpressions.isEmpty()) {
			description += " fields " + Arrays.toString(fieldExpressions.keySet().toArray());
		}
		return description;
	}

}
