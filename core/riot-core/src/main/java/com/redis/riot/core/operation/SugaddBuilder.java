package com.redis.riot.core.operation;

import java.util.Map;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;

import com.redis.lettucemod.search.Suggestion;
import com.redis.spring.batch.common.ToSuggestionFunction;
import com.redis.spring.batch.writer.operation.Sugadd;

public class SugaddBuilder extends AbstractMapOperationBuilder {

    public static final double DEFAULT_SCORE = 1;

    public static final boolean DEFAULT_INCREMENT = false;

    private String stringField;

    private String scoreField;

    private double defaultScore = DEFAULT_SCORE;

    private String payloadField;

    private boolean increment = DEFAULT_INCREMENT;

    public void setStringField(String stringField) {
        this.stringField = stringField;
    }

    public void setScoreField(String scoreField) {
        this.scoreField = scoreField;
    }

    public void setDefaultScore(double defaultScore) {
        this.defaultScore = defaultScore;
    }

    public void setPayloadField(String payloadField) {
        this.payloadField = payloadField;
    }

    public void setIncrement(boolean increment) {
        this.increment = increment;
    }

    @Override
    protected Sugadd<String, String, Map<String, Object>> operation() {
        Sugadd<String, String, Map<String, Object>> operation = new Sugadd<>();
        operation.setSuggestionFunction(suggestion());
        operation.setIncr(increment);
        return operation;
    }

    private Function<Map<String, Object>, Suggestion<String>> suggestion() {
        Function<Map<String, Object>, String> string = toString(stringField);
        ToDoubleFunction<Map<String, Object>> score = toDouble(scoreField, defaultScore);
        Function<Map<String, Object>, String> payload = toString(payloadField);
        return new ToSuggestionFunction<>(string, score, payload);
    }

}
