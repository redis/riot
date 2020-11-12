package com.redislabs.riot.stream;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.batch.item.redis.RedisStreamItemWriter;
import org.springframework.batch.item.redis.support.ConstantConverter;
import org.springframework.batch.item.redis.support.Transfer;
import org.springframework.core.convert.converter.Converter;

import com.redislabs.riot.convert.MapFlattener;
import com.redislabs.riot.convert.ObjectToStringConverter;
import com.redislabs.riot.stream.kafka.KafkaItemReader;
import com.redislabs.riot.stream.kafka.KafkaItemReaderBuilder;

import io.lettuce.core.XAddArgs;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "import", description = "Import Kafka topics into Redis streams")
public class StreamImportCommand
	extends AbstractStreamCommand<ConsumerRecord<String, Object>, ConsumerRecord<String, Object>> {

    @Parameters(arity = "1..*", description = "One ore more topics to read from", paramLabel = "TOPIC")
    private List<String> topics;
    @Option(names = "--key", description = "Target stream key (default: same as topic)", paramLabel = "<string>")
    private String key;
    @Option(names = "--maxlen", description = "Stream maxlen", paramLabel = "<int>")
    private Long maxlen;
    @Option(names = "--trim", description = "Stream efficient trimming ('~' flag)")
    private boolean approximateTrimming;

    @Override
    protected List<Transfer<ConsumerRecord<String, Object>, ConsumerRecord<String, Object>>> transfers()
	    throws Exception {
	List<Transfer<ConsumerRecord<String, Object>, ConsumerRecord<String, Object>>> transfers = new ArrayList<>();
	RedisStreamItemWriter<ConsumerRecord<String, Object>> writer = configure(
		RedisStreamItemWriter.<ConsumerRecord<String, Object>>builder().keyConverter(keyConverter())
			.argsConverter(new ConstantConverter<>(xAddArgs())).bodyConverter(bodyConverter())).build();
	for (String topic : topics) {
	    KafkaItemReader<String, Object> reader = new KafkaItemReaderBuilder<String, Object>().partitions(0)
		    .consumerProperties(kafkaOptions.consumerProperties()).partitions(0).name(topic).saveState(false)
		    .topic(topic).build();
	    transfers.add(transfer(reader, null, writer));
	}
	return transfers;
    }

    private Converter<ConsumerRecord<String, Object>, Map<String, String>> bodyConverter() {
	switch (kafkaOptions.getSerde()) {
	case JSON:
	    return new JsonToMapConverter();
	default:
	    return new AvroToMapConverter();
	}
    }

    static class JsonToMapConverter implements Converter<ConsumerRecord<String, Object>, Map<String, String>> {

	private Converter<Map<String, Object>, Map<String, String>> flattener = new MapFlattener<String>(
		new ObjectToStringConverter());

	@Override
	@SuppressWarnings("unchecked")
	public Map<String, String> convert(ConsumerRecord<String, Object> source) {
	    return flattener.convert((Map<String, Object>) source.value());
	}

    }

    static class AvroToMapConverter implements Converter<ConsumerRecord<String, Object>, Map<String, String>> {

	private Converter<Map<String, Object>, Map<String, String>> flattener = new MapFlattener<String>(
		new ObjectToStringConverter());

	@Override
	public Map<String, String> convert(ConsumerRecord<String, Object> source) {
	    GenericRecord record = (GenericRecord) source.value();
	    Map<String, Object> map = new HashMap<>();
	    record.getSchema().getFields().forEach(field -> map.put(field.name(), record.get(field.name())));
	    return flattener.convert(map);
	}

    }

    private Converter<ConsumerRecord<String, Object>, String> keyConverter() {
	if (key == null) {
	    return ConsumerRecord::topic;
	}
	return new ConstantConverter<>(key);
    }

    private XAddArgs xAddArgs() {
	if (maxlen == null) {
	    return null;
	}
	XAddArgs args = new XAddArgs();
	args.maxlen(maxlen);
	args.approximateTrimming(approximateTrimming);
	return args;
    }

}
