package com.redis.riot.core;

import java.util.Map;
import java.util.regex.Pattern;

import org.springframework.batch.core.Job;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.function.FunctionItemProcessor;

import com.redis.riot.core.function.RegexNamedGroupFunction;
import com.redis.riot.core.function.StructToMapFunction;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.reader.StructItemReader;
import com.redis.spring.batch.util.BatchUtils;

import io.lettuce.core.codec.StringCodec;

public abstract class AbstractMapExport extends AbstractExport {

    public static final Pattern DEFAULT_KEY_PATTERN = Pattern.compile("\\w+:(?<id>.+)");

    private Pattern keyPattern = DEFAULT_KEY_PATTERN;

    public void setKeyPattern(Pattern pattern) {
        this.keyPattern = pattern;
    }

    @Override
    protected Job job(RiotContext context) {
        StepBuilder<KeyValue<String>, Map<String, Object>> step = createStep();
        step.name(getName());
        step.reader(reader(context.getRedisContext()));
        step.writer(writer());
        step.processor(processor(context));
        return jobBuilder().start(step.build()).build();
    }

    protected StructItemReader<String, String> reader(RedisContext context) {
        StructItemReader<String, String> reader = new StructItemReader<>(context.getClient(), StringCodec.UTF8);
        configureReader(reader, context);
        return reader;
    }

    private ItemProcessor<KeyValue<String>, Map<String, Object>> processor(RiotContext context) {
        ItemProcessor<KeyValue<String>, KeyValue<String>> processor = processor(StringCodec.UTF8, context);
        StructToMapFunction toMapFunction = new StructToMapFunction();
        if (keyPattern != null) {
            toMapFunction.setKey(new RegexNamedGroupFunction(keyPattern));
        }
        return BatchUtils.processor(processor, new FunctionItemProcessor<>(toMapFunction));
    }

    protected abstract ItemWriter<Map<String, Object>> writer();

}
