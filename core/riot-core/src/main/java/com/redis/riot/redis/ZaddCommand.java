package com.redis.riot.redis;

import io.lettuce.core.ScoredValue;
import org.springframework.batch.item.redis.support.convert.KeyMaker;
import org.springframework.batch.item.redis.support.operation.Zadd;
import org.springframework.core.convert.converter.Converter;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.Map;

@Command(name = "zadd", description = "Add members with scores to a sorted set")
public class ZaddCommand extends AbstractCollectionCommand {

    @SuppressWarnings("unused")
    @Option(names = "--score", description = "Name of the field to use for scores", paramLabel = "<field>")
    private String scoreField;
    @Option(names = "--score-default", description = "Score when field not present (default: ${DEFAULT-VALUE})", paramLabel = "<num>")
    private double scoreDefault = 1;

    @Override
    public Zadd<String, String, Map<String, Object>> operation() {
        return Zadd.key(key()).value(new ScoredValueConverter(member(), numberFieldExtractor(Double.class, scoreField, scoreDefault))).build();
    }

    private static class ScoredValueConverter implements Converter<Map<String, Object>, ScoredValue<String>> {

        private final Converter<Map<String, Object>, String> member;
        private final Converter<Map<String, Object>, Double> score;

        public ScoredValueConverter(KeyMaker<Map<String, Object>> member, Converter<Map<String, Object>, Double> score) {
            this.member = member;
            this.score = score;
        }

        @Override
        public ScoredValue<String> convert(Map<String, Object> source) {
            Double score = this.score.convert(source);
            if (score == null) {
                return null;
            }
            return ScoredValue.just(score, member.convert(source));
        }
    }

}
