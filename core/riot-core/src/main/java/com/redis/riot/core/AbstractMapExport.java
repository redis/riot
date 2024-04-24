package com.redis.riot.core;

import java.util.Map;
import java.util.regex.Pattern;

import org.springframework.batch.core.Job;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.function.FunctionItemProcessor;

import com.redis.riot.core.function.RegexNamedGroupFunction;
import com.redis.riot.core.function.StructToMapFunction;
import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.RedisItemReader;

import io.lettuce.core.codec.StringCodec;

public abstract class AbstractMapExport extends AbstractExport {

	public static final Pattern DEFAULT_KEY_REGEX = Pattern.compile("\\w+:(?<id>.+)");

	private Pattern keyRegex = DEFAULT_KEY_REGEX;

	public void setKeyRegex(Pattern pattern) {
		this.keyRegex = pattern;
	}

	@Override
	protected Job job() {
		return jobBuilder().start(step(getName(), reader(), writer()).processor(processor()).build()).build();
	}

	protected RedisItemReader<String, String, KeyValue<String, Object>> reader() {
		RedisItemReader<String, String, KeyValue<String, Object>> reader = RedisItemReader.struct();
		configure(reader);
		return reader;
	}

	protected ItemProcessor<KeyValue<String, Object>, Map<String, Object>> processor() {
		ItemProcessor<KeyValue<String, Object>, KeyValue<String, Object>> processor = processor(StringCodec.UTF8);
		StructToMapFunction toMapFunction = new StructToMapFunction();
		if (keyRegex != null) {
			toMapFunction.setKey(new RegexNamedGroupFunction(keyRegex));
		}
		return RiotUtils.processor(processor, new FunctionItemProcessor<>(toMapFunction));
	}

	@Override
	protected boolean isStruct() {
		return true;
	}

	protected abstract ItemWriter<Map<String, Object>> writer();

}
