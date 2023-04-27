package com.redis.riot.cli.operation;

import java.util.Map;

import org.springframework.core.convert.converter.Converter;

import com.redis.lettucemod.search.Suggestion;
import com.redis.spring.batch.convert.SuggestionConverter;
import com.redis.spring.batch.writer.operation.Sugadd;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command(name = "ft.sugadd", description = "Add suggestion strings to a RediSearch auto-complete dictionary")
public class SugaddCommand extends AbstractKeyCommand {

	@Mixin
	private SugaddOptions options = new SugaddOptions();

	@Override
	public Sugadd<String, String, Map<String, Object>> operation() {
		return Sugadd.<String, Map<String, Object>>key(key()).suggestion(suggestion()).increment(options.isIncrement())
				.build();
	}

	private Converter<Map<String, Object>, Suggestion<String>> suggestion() {
		return new SuggestionConverter<>(stringFieldExtractor(options.getField()),
				numberExtractor(options.getScoreField(), Double.class, options.getScoreDefault()),
				stringFieldExtractor(options.getPayload()));
	}

}
