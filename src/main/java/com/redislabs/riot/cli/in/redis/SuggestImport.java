package com.redislabs.riot.cli.in.redis;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;

import com.redislabs.riot.redis.writer.search.AbstractLettuSearchItemWriter;
import com.redislabs.riot.redis.writer.search.LettuSearchSuggestPayloadWriter;
import com.redislabs.riot.redis.writer.search.LettuSearchSuggestWriter;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "suggest", description = "Suggestion index")
public class SuggestImport extends AbstractRediSearchImport {

	@Option(names = "--score", description = "Name of the field to use for scores.")
	private String scoreField;
	@Option(names = "--default-score", description = "Default score to use when score field is not present. (default: ${DEFAULT-VALUE}).")
	private Double defaultScore = 1d;
	@Option(names = "--increment", description = "Use increment to set value")
	private boolean increment;
	@Option(names = "--suggest", description = "Name of the field containing the suggestion")
	private String suggestField;
	@Option(names = "--payload", description = "Name of the field containing the payload")
	private String payloadField;

	@Override
	protected AbstractLettuSearchItemWriter rediSearchItemWriter() {
		if (payloadField == null) {
			LettuSearchSuggestWriter writer = new LettuSearchSuggestWriter();
			writer.setDefaultScore(defaultScore);
			writer.setField(suggestField);
			writer.setIncrement(increment);
			writer.setScoreField(scoreField);
			return writer;
		}
		LettuSearchSuggestPayloadWriter writer = new LettuSearchSuggestPayloadWriter();
		writer.setDefaultScore(defaultScore);
		writer.setField(suggestField);
		writer.setIncrement(increment);
		writer.setPayloadField(payloadField);
		writer.setScoreField(scoreField);
		return writer;
	}

	@Override
	protected ItemWriter<Map<String, Object>> jedisSearchWriter() {
		return null;
	}

}
