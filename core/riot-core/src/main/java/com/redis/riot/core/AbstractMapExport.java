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

import io.lettuce.core.codec.StringCodec;

public abstract class AbstractMapExport extends AbstractExport {

    public static final Pattern DEFAULT_KEY_PATTERN = Pattern.compile("\\w+:(?<id>.+)");

    private Pattern keyPattern = DEFAULT_KEY_PATTERN;

    public void setKeyPattern(Pattern pattern) {
        this.keyPattern = pattern;
    }

    @Override
    protected Job job(RiotContext context) {
        StructItemReader<String, String> reader = reader(context.getRedisContext());
        ItemProcessor<KeyValue<String>, Map<String, Object>> processor = processor(context);
        ItemWriter<Map<String, Object>> writer = writer();
        return jobBuilder().start(step(getName(), reader, processor, writer).build()).build();
    }

    protected StructItemReader<String, String> reader(RedisContext context) {
        StructItemReader<String, String> reader = new StructItemReader<>(context.getClient(), StringCodec.UTF8);
        configureReader(reader, context);
        return reader;
    }

    protected ItemProcessor<KeyValue<String>, Map<String, Object>> processor(RiotContext context) {
        ItemProcessor<KeyValue<String>, KeyValue<String>> processor = processor(StringCodec.UTF8, context);
        StructToMapFunction toMapFunction = new StructToMapFunction();
        if (keyPattern != null) {
            toMapFunction.setKey(new RegexNamedGroupFunction(keyPattern));
        }
        return RiotUtils.processor(processor, new FunctionItemProcessor<>(toMapFunction));
    }
    
    @Override
    protected boolean isStruct() {
        return true;
    }

    protected abstract ItemWriter<Map<String, Object>> writer();

}
