package com.redis.riot.core;

import java.util.Map;
import java.util.regex.Pattern;

import org.springframework.batch.core.Job;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.function.FunctionItemProcessor;

import com.redis.riot.core.function.KeyValueToMapFunction;
import com.redis.riot.core.function.RegexNamedGroupFunction;
import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.util.BatchUtils;

import io.lettuce.core.codec.StringCodec;

public abstract class AbstractMapExport extends AbstractExport<String, String> {

    public static final Pattern DEFAULT_KEY_PATTERN = Pattern.compile("\\w+:(?<id>.+)");

    private Pattern keyPattern = DEFAULT_KEY_PATTERN;

    protected AbstractMapExport() {
        super(StringCodec.UTF8);
    }

    public void setKeyPattern(Pattern pattern) {
        this.keyPattern = pattern;
    }

    @Override
    protected Job job(RiotExecutionContext context) {
        StepBuilder<KeyValue<String>, Map<String, Object>> step = createStep();
        step.name(getName());
        step.reader(reader(context, StringCodec.UTF8));
        step.writer(writer());
        step.processor(processor(context));
        return jobBuilder().start(step.build()).build();
    }

    private ItemProcessor<KeyValue<String>, Map<String, Object>> processor(RiotExecutionContext context) {
        KeyValueToMapFunction function = new KeyValueToMapFunction();
        if (keyPattern != null) {
            function.setKey(new RegexNamedGroupFunction(keyPattern));
        }
        FunctionItemProcessor<KeyValue<String>, Map<String, Object>> toMapProcessor = new FunctionItemProcessor<>(function);
        ItemProcessor<KeyValue<String>, KeyValue<String>> keyValueProcessor = keyValueProcessor(context);
        if (keyValueProcessor == null) {
            return toMapProcessor;
        }
        return BatchUtils.processor(keyValueProcessor, toMapProcessor);
    }

    protected abstract ItemWriter<Map<String, Object>> writer();

}
