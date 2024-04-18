package com.redis.riot.file;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.springframework.util.StringUtils;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.KeyValue.Type;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

public class KeyValueDeserializer extends StdDeserializer<KeyValue<String, Object>> {

	private static final long serialVersionUID = 1L;

	public static final String KEY = "key";
	public static final String TYPE = "type";
	public static final String VALUE = "value";
	public static final String SCORE = "score";
	public static final String TTL = "ttl";
	public static final String MEMORY_USAGE = "memoryUsage";
	public static final String STREAM = "stream";
	public static final String ID = "id";
	public static final String BODY = "body";
	public static final String TIMESTAMP = "timestamp";

	public KeyValueDeserializer() {
		this(null);
	}

	public KeyValueDeserializer(Class<KeyValue<String, Object>> t) {
		super(t);
	}

	@Override
	public KeyValue<String, Object> deserialize(JsonParser p, DeserializationContext ctxt)
			throws IOException, JacksonException {
		JsonNode node = p.getCodec().readTree(p);
		KeyValue<String, Object> keyValue = new KeyValue<>();
		JsonNode keyNode = node.get(KEY);
		if (keyNode != null) {
			keyValue.setKey(node.get(KEY).asText());
		}
		JsonNode typeNode = node.get(TYPE);
		if (typeNode != null) {
			String typeString = typeNode.asText();
			if (StringUtils.hasLength(typeString)) {
				keyValue.setType(Type.valueOf(typeString.toUpperCase()));
			}
		}
		LongNode ttlNode = (LongNode) node.get(TTL);
		if (ttlNode != null) {
			keyValue.setTtl(ttlNode.asLong());
		}
		LongNode memUsageNode = (LongNode) node.get(MEMORY_USAGE);
		if (memUsageNode != null) {
			keyValue.setMem(memUsageNode.asLong());
		}
		keyValue.setValue(value(keyValue.getType(), node.get(VALUE), ctxt));
		return keyValue;
	}

	private Object value(Type type, JsonNode node, DeserializationContext ctxt) throws IOException {
		switch (type) {
		case STREAM:
			return streamMessages((ArrayNode) node, ctxt);
		case ZSET:
			return scoredValues((ArrayNode) node);
		case TIMESERIES:
			return samples((ArrayNode) node);
		case HASH:
			return ctxt.readTreeAsValue(node, Map.class);
		case STRING:
		case JSON:
			return node.asText();
		case LIST:
			return ctxt.readTreeAsValue(node, Collection.class);
		case SET:
			return ctxt.readTreeAsValue(node, Set.class);
		default:
			return null;
		}
	}

	private Collection<Sample> samples(ArrayNode node) {
		Collection<Sample> samples = new ArrayList<>(node.size());
		for (int index = 0; index < node.size(); index++) {
			JsonNode sampleNode = node.get(index);
			if (sampleNode != null) {
				samples.add(sample(sampleNode));
			}
		}
		return samples;
	}

	private Sample sample(JsonNode node) {
		LongNode timestampNode = (LongNode) node.get(TIMESTAMP);
		long timestamp = timestampNode == null || timestampNode.isNull() ? 0 : timestampNode.asLong();
		DoubleNode valueNode = (DoubleNode) node.get(VALUE);
		double value = valueNode == null || valueNode.isNull() ? 0 : valueNode.asDouble();
		return Sample.of(timestamp, value);
	}

	private Collection<ScoredValue<String>> scoredValues(ArrayNode node) {
		Collection<ScoredValue<String>> scoredValues = new ArrayList<>(node.size());
		for (int index = 0; index < node.size(); index++) {
			JsonNode scoredValueNode = node.get(index);
			if (scoredValueNode != null) {
				scoredValues.add(scoredValue(scoredValueNode));
			}
		}
		return scoredValues;
	}

	private ScoredValue<String> scoredValue(JsonNode scoredValueNode) {
		JsonNode valueNode = scoredValueNode.get(VALUE);
		String value = valueNode == null || valueNode.isNull() ? null : valueNode.asText();
		DoubleNode scoreNode = (DoubleNode) scoredValueNode.get(SCORE);
		double score = scoreNode == null || scoreNode.isNull() ? 0 : scoreNode.asDouble();
		return ScoredValue.just(score, value);
	}

	private Collection<StreamMessage<String, String>> streamMessages(ArrayNode node, DeserializationContext ctxt)
			throws IOException {
		Collection<StreamMessage<String, String>> messages = new ArrayList<>(node.size());
		for (int index = 0; index < node.size(); index++) {
			JsonNode messageNode = node.get(index);
			if (messageNode != null) {
				messages.add(streamMessage(messageNode, ctxt));
			}
		}
		return messages;
	}

	@SuppressWarnings("unchecked")
	private StreamMessage<String, String> streamMessage(JsonNode messageNode, DeserializationContext ctxt)
			throws IOException {
		JsonNode streamNode = messageNode.get(STREAM);
		String stream = streamNode == null || streamNode.isNull() ? null : streamNode.asText();
		JsonNode bodyNode = messageNode.get(BODY);
		Map<String, String> body = ctxt.readTreeAsValue(bodyNode, Map.class);
		JsonNode idNode = messageNode.get(ID);
		String id = idNode == null || idNode.isNull() ? null : idNode.asText();
		return new StreamMessage<>(stream, id, body);
	}

}
