package com.redis.riot.redis;

import java.util.Map;
import java.util.Optional;

import org.springframework.core.convert.converter.Converter;

import com.redis.lettucemod.search.Suggestion;
import com.redis.spring.batch.convert.SuggestionConverter;
import com.redis.spring.batch.writer.RedisOperation;
import com.redis.spring.batch.writer.operation.Sugadd;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "ft.sugadd", description = "Add suggestion strings to a RediSearch auto-complete suggestion dictionary")
public class SugaddCommand extends AbstractKeyCommand {

	@Option(names = "--field", required = true, description = "Field containing the strings to add", paramLabel = "<field>")
	private String field;
	@Option(names = "--score", description = "Name of the field to use for scores", paramLabel = "<field>")
	private Optional<String> scoreField = Optional.empty();
	@Option(names = "--score-default", description = "Score when field not present (default: ${DEFAULT-VALUE})", paramLabel = "<num>")
	private double scoreDefault = 1;
	@Option(names = "--payload", description = "Field containing the payload", paramLabel = "<field>")
	private Optional<String> payload = Optional.empty();
	@Option(names = "--increment", description = "Increment the existing suggestion by the score instead of replacing the score")
	private boolean increment;

	@Override
	public RedisOperation<String, String, Map<String, Object>> operation() {
		return Sugadd.<String, String, Map<String, Object>>key(key()).suggestion(suggestion()).increment(increment)
				.build();
	}

	private Converter<Map<String, Object>, Suggestion<String>> suggestion() {
		return new SuggestionConverter<>(stringFieldExtractor(field),
				numberExtractor(scoreField, Double.class, scoreDefault), stringFieldExtractor(payload));
	}

}
