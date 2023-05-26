package com.redis.riot.cli.operation;

import java.util.Map;
import java.util.function.Function;

import com.redis.lettucemod.search.Suggestion;
import com.redis.spring.batch.convert.SuggestionConverter;
import com.redis.spring.batch.writer.operation.Sugadd;
import com.redis.spring.batch.writer.operation.SugaddIncr;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command(name = "ft.sugadd", description = "Add suggestion strings to a RediSearch auto-complete dictionary")
public class SugaddCommand extends AbstractKeyCommand {

	@Mixin
	private SugaddOptions options = new SugaddOptions();

	@Override
	public Sugadd<String, String, Map<String, Object>> operation() {
		if (options.isIncrement()) {
			return new SugaddIncr<>(key(), suggestion());
		}
		return new Sugadd<>(key(), suggestion());
	}

	private Function<Map<String, Object>, Suggestion<String>> suggestion() {
		return new SuggestionConverter<>(stringFieldExtractor(options.getField()),
				numberExtractor(options.getScore(), Double.class, options.getDefaultScore()),
				stringFieldExtractor(options.getPayload()));
	}

}
