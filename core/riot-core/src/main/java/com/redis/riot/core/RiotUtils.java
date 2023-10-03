package com.redis.riot.core;

import java.util.List;
import java.util.function.Predicate;

import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.common.TemplateParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.CollectionUtils;

import com.redis.spring.batch.util.CodecUtils;
import com.redis.spring.batch.util.Predicates;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public abstract class RiotUtils {

    private static final SpelExpressionParser parser = new SpelExpressionParser();

    private RiotUtils() {
    }

    public static Expression parse(String expressionString) {
        return parser.parseExpression(expressionString);
    }

    public static TemplateExpression parseTemplate(String expressionString) {
        TemplateExpression expression = new TemplateExpression();
        expression.setExpression(parser.parseExpression(expressionString, new TemplateParserContext()));
        return expression;
    }

    public static <T> Predicate<T> predicate(EvaluationContext context, Expression expression) {
        return t -> expression.getValue(context, t, Boolean.class);
    }

    public static Predicate<String> keyFilterPredicate(KeyFilterOptions options) {
        return keyFilterPredicate(StringCodec.UTF8, options);
    }

    public static <K> Predicate<K> keyFilterPredicate(RedisCodec<K, ?> codec, KeyFilterOptions options) {
        Predicate<K> globPredicate = globPredicate(codec, options);
        Predicate<K> slotsPredicate = slotsPredicate(codec, options);
        if (globPredicate == null) {
            if (slotsPredicate == null) {
                return null;
            }
            return slotsPredicate;
        }
        if (slotsPredicate == null) {
            return globPredicate;
        }
        return slotsPredicate.and(globPredicate);
    }

    public static <K> Predicate<K> slotsPredicate(RedisCodec<K, ?> codec, KeyFilterOptions options) {
        if (CollectionUtils.isEmpty(options.getSlots())) {
            return null;
        }
        return Predicates.slots(codec, options.getSlots());
    }

    public static <K> Predicate<K> globPredicate(RedisCodec<K, ?> codec, KeyFilterOptions options) {
        Predicate<String> predicate = globPredicate(options);
        if (predicate == null) {
            return null;
        }
        return Predicates.map(CodecUtils.toStringKeyFunction(codec), predicate);
    }

    public static Predicate<String> globPredicate(KeyFilterOptions options) {
        if (CollectionUtils.isEmpty(options.getIncludes())) {
            if (CollectionUtils.isEmpty(options.getExcludes())) {
                return null;
            }
            return globPredicate(options.getExcludes()).negate();
        }
        if (CollectionUtils.isEmpty(options.getExcludes())) {
            return globPredicate(options.getIncludes());
        }
        return globPredicate(options.getIncludes()).and(globPredicate(options.getExcludes()).negate());
    }

    public static Predicate<String> globPredicate(List<String> patterns) {
        if (CollectionUtils.isEmpty(patterns)) {
            return Predicates.isTrue();
        }
        return Predicates.or(patterns.stream().map(Predicates::glob));
    }

}
