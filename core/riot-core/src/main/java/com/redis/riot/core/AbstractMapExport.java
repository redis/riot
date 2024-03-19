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

	public static final Pattern DEFAULT_KEY_REGEX = Pattern.compile("\\w+:(?<id>.+)");

	private Pattern keyRegex = DEFAULT_KEY_REGEX;

	public void setKeyRegex(Pattern pattern) {
		this.keyRegex = pattern;
	}

	@Override
	protected Job job() {
		StructItemReader<String, String> reader = reader();
		ItemProcessor<KeyValue<String>, Map<String, Object>> processor = processor();
		ItemWriter<Map<String, Object>> writer = writer();
		return jobBuilder().start(step(reader, processor, writer)).build();
	}

	protected StructItemReader<String, String> reader() {
		StructItemReader<String, String> reader = new StructItemReader<>(getRedisClient(), StringCodec.UTF8);
		configureReader("export-reader", reader);
		return reader;
	}

	protected ItemProcessor<KeyValue<String>, Map<String, Object>> processor() {
		ItemProcessor<KeyValue<String>, KeyValue<String>> processor = new FunctionItemProcessor<>(
				processor(StringCodec.UTF8));
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
