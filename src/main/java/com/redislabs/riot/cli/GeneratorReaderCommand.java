package com.redislabs.riot.cli;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import com.redislabs.riot.generator.AbstractGeneratorReader;
import com.redislabs.riot.generator.FakerGeneratorReader;
import com.redislabs.riot.generator.SimpleGeneratorReader;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "gen", description = "Data generator")
public class GeneratorReaderCommand extends AbstractReaderCommand {

	static enum Generator {
		faker, simple
	}

	@Parameters(arity = "1..*", description = "Fields to generate", paramLabel = "<name=expression>")
	private Map<String, String> fields;
	@Option(names = "--locale", description = "Faker locale")
	private Locale locale = Locale.ENGLISH;
	@Option(names = "--type", description = "Type of data generator to use", paramLabel = "<type>")
	private Generator generatorType = Generator.faker;

	@Override
	protected AbstractGeneratorReader reader() {
		switch (generatorType) {
		case simple:
			return new SimpleGeneratorReader(simpleFields());
		default:
			SpelExpressionParser parser = new SpelExpressionParser();
			Map<String, Expression> expressionMap = new LinkedHashMap<String, Expression>();
			fields.forEach((k, v) -> expressionMap.put(k, parser.parseExpression(v)));
			return new FakerGeneratorReader(locale, expressionMap);
		}
	}

	private Map<String, Integer> simpleFields() {
		Map<String, Integer> map = new LinkedHashMap<>();
		fields.forEach((k, v) -> map.put(k, Integer.parseInt(v)));
		return map;
	}

	@Override
	protected String description() {
		String description = "generated";
		if (fields == null) {
			description += " fields " + Arrays.toString(fields.keySet().toArray());
		}
		return description;
	}

}
