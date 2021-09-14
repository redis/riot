package com.redis.riot.redis;

import com.redis.lettucemod.api.search.Suggestion;
import org.springframework.batch.item.redis.support.RedisOperation;
import org.springframework.batch.item.redis.support.operation.Sugadd;
import org.springframework.core.convert.converter.Converter;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.Map;

@Command(name = "sugadd", description = "Add suggestion strings to a RediSearch auto-complete suggestion dictionary")
public class SugaddCommand extends AbstractKeyCommand {

    @Option(names = "--field", required = true, description = "Field containing the strings to add", paramLabel = "<field>")
    private String field;
    @SuppressWarnings("unused")
    @Option(names = "--score", description = "Name of the field to use for scores", paramLabel = "<field>")
    private String scoreField;
    @Option(names = "--score-default", description = "Score when field not present (default: ${DEFAULT-VALUE})", paramLabel = "<num>")
    private double scoreDefault = 1;
    @Option(names = "--payload", description = "Field containing the payload", paramLabel = "<field>")
    private String payload;
    @Option(names = "--increment", description = "Increment the existing suggestion by the score instead of replacing the score")
    private boolean increment;

    @Override
    public RedisOperation<String, String, Map<String, Object>> operation() {
        return Sugadd.key(key()).suggestion(suggestion()).increment(increment).build();
    }

    private Converter<Map<String, Object>, Suggestion<String>> suggestion() {
        Converter<Map<String, Object>, String> string = stringFieldExtractor(field);
        Converter<Map<String, Object>, Double> score = numberFieldExtractor(Double.class, scoreField, scoreDefault);
        Converter<Map<String, Object>, String> payload = stringFieldExtractor(this.payload);
        return new SuggestionConverter(string, score, payload);
    }

    private static class SuggestionConverter implements Converter<Map<String,Object>, Suggestion<String>> {

        private Converter<Map<String, Object>, String> string;
        private Converter<Map<String, Object>, Double> score;
        private Converter<Map<String, Object>, String> payload;

        public SuggestionConverter(Converter<Map<String, Object>, String> string, Converter<Map<String, Object>, Double> score, Converter<Map<String, Object>, String> payload) {
            this.string = string;
            this.score = score;
            this.payload = payload;
        }

        @Override
        public Suggestion<String> convert(Map<String, Object> source) {
            Suggestion<String> suggestion = new Suggestion<>();
            suggestion.setString(string.convert(source));
            suggestion.setScore(score.convert(source));
            suggestion.setPayload(payload.convert(source));
            return suggestion;
        }
    }


}
