package com.redis.riot.core.operation;

import java.util.Map;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;

import com.redis.lettucemod.search.Suggestion;
import com.redis.spring.batch.util.ToSuggestionFunction;
import com.redis.spring.batch.writer.operation.Sugadd;

public class SugaddBuilder extends AbstractMapOperationBuilder<SugaddBuilder> {

    public static final double DEFAULT_SCORE = 1;

    public static final boolean DEFAULT_INCREMENT = false;

    private String stringField;

    private String scoreField;

    private double defaultScore = DEFAULT_SCORE;

    private String payloadField;

    private boolean increment = DEFAULT_INCREMENT;

    public SugaddBuilder string(String field) {
        this.stringField = field;
        return this;
    }

    public SugaddBuilder score(String field) {
        this.scoreField = field;
        return this;
    }

    public SugaddBuilder defaultScore(double score) {
        this.defaultScore = score;
        return this;
    }

    public SugaddBuilder payload(String field) {
        this.payloadField = field;
        return this;
    }

    public SugaddBuilder increment(boolean increment) {
        this.increment = increment;
        return this;
    }

    @Override
    protected Sugadd<String, String, Map<String, Object>> operation() {
        return new Sugadd<String, String, Map<String, Object>>().suggestion(suggestion()).incr(increment);
    }

    private Function<Map<String, Object>, Suggestion<String>> suggestion() {
        Function<Map<String, Object>, String> string = toString(stringField);
        ToDoubleFunction<Map<String, Object>> score = toDouble(scoreField, defaultScore);
        Function<Map<String, Object>, String> payload = toString(payloadField);
        return new ToSuggestionFunction<>(string, score, payload);
    }

}
