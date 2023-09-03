package com.redis.riot.core.operation;

import java.util.Map;
import java.util.function.ToDoubleFunction;

import com.redis.spring.batch.util.ToScoredValueFunction;
import com.redis.spring.batch.writer.operation.Zadd;

public class ZaddSupplier extends AbstractCollectionMapOperationBuilder<ZaddSupplier> {

    public static final double DEFAULT_SCORE = 1;

    private String scoreField;

    private double defaultScore = DEFAULT_SCORE;

    @Override
    public Zadd<String, String, Map<String, Object>> operation() {
        return new Zadd<String, String, Map<String, Object>>().value(value());
    }

    private ToScoredValueFunction<String, Map<String, Object>> value() {
        return new ToScoredValueFunction<>(member(), score());
    }

    private ToDoubleFunction<Map<String, Object>> score() {
        return toDouble(scoreField, defaultScore);
    }

    public String getScoreField() {
        return scoreField;
    }

    public void setScoreField(String scoreField) {
        this.scoreField = scoreField;
    }

    public double getDefaultScore() {
        return defaultScore;
    }

    public void setDefaultScore(double defaultScore) {
        this.defaultScore = defaultScore;
    }

}
