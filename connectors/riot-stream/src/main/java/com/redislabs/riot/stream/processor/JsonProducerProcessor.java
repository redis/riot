package com.redislabs.riot.stream.processor;

import java.util.Map;

import org.springframework.core.convert.converter.Converter;

import io.lettuce.core.StreamMessage;

public class JsonProducerProcessor extends AbstractProducerProcessor {

	public JsonProducerProcessor(Converter<StreamMessage<String, String>, String> topicConverter) {
		super(topicConverter);
	}

	@Override
	protected Map<String, String> value(StreamMessage<String, String> message) {
		return message.getBody();
	}

}
