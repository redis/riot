package com.redis.riot.gen;

import lombok.Builder;
import org.springframework.batch.item.ItemReader;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.util.Assert;

import java.util.Map;

/**
 * {@link ItemReader} that generates HashMaps using Faker.
 *
 * @author Julien Ruaux
 */
public class FakerItemReader implements ItemReader<Map<String, Object>> {

    private final long start;
    private final long end;
    private final Generator<Map<String, Object>> generator;

    private EvaluationContext context;
    private Map<String, Expression> expressions;
    private long currentItemCount;

    @Builder
    private FakerItemReader(Generator<Map<String, Object>> generator, long start, long end) {
        Assert.notNull(generator, "A generator is required");
        Assert.isTrue(end > start, "End index must be strictly greater than start index");
        this.generator = generator;
        this.start = start;
        this.end = end;
    }

    @Override
    public Map<String, Object> read() {
        if (currentItemCount >= end - start) {
            return null;
        }
        currentItemCount++;
        return generator.next(start + (currentItemCount % (end - start)));
    }

}
