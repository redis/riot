package com.redislabs.riot.stream;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.RedisStreamItemWriter;
import org.springframework.core.convert.converter.Converter;

import com.redislabs.riot.AbstractFlushingTransferCommand;
import com.redislabs.riot.stream.kafka.KafkaItemReader;
import com.redislabs.riot.stream.kafka.KafkaItemReaderBuilder;
import com.redislabs.riot.stream.processor.AvroConsumerProcessor;
import com.redislabs.riot.stream.processor.JsonConsumerProcessor;

import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "import", description = "Import Kafka topics into Redis streams")
public class StreamImportCommand
		extends AbstractFlushingTransferCommand<ConsumerRecord<String, Object>, StreamMessage<String, String>> {

	@Option(names = "--key", description = "Target stream key (default: same as topic)", paramLabel = "<string>")
	private String key;
	@Option(names = "--maxlen", description = "Stream maxlen", paramLabel = "<int>")
	private Long maxlen;
	@Option(names = "--trim", description = "Stream efficient trimming ('~' flag)")
	private boolean approximateTrimming;
	@Parameters(arity = "1..*", description = "One ore more topics to read from", paramLabel = "TOPIC")
	private List<String> topics;
	@Mixin
	private KafkaOptions kafkaOptions = new KafkaOptions();

	@Override
	protected String taskName() {
		return "Streaming from";
	}

	@Override
	protected List<ItemReader<ConsumerRecord<String, Object>>> readers() throws Exception {
		return topics.stream().map(this::reader).collect(Collectors.toList());
	}

	@Override
	protected ItemProcessor<ConsumerRecord<String, Object>, StreamMessage<String, String>> processor() {
		switch (kafkaOptions.getSerde()) {
		case JSON:
			return new JsonConsumerProcessor<>(keyConverter());
		default:
			return new AvroConsumerProcessor<>(keyConverter());
		}
	}

	private Converter<ConsumerRecord<String, Object>, String> keyConverter() {
		if (key == null) {
			return ConsumerRecord::topic;
		}
		return r -> key;
	}

	@Override
	protected ItemWriter<StreamMessage<String, String>> writer() throws Exception {
		return configure(RedisStreamItemWriter.builder().converter(xAddArgsConverter())).build();
	}

	private Converter<StreamMessage<String, String>, XAddArgs> xAddArgsConverter() {
		if (maxlen == null) {
			return null;
		}
		XAddArgs args = new XAddArgs();
		args.maxlen(maxlen);
		args.approximateTrimming(approximateTrimming);
		return m -> args;
	}

	private KafkaItemReader<String, Object> reader(String topic) {
		return new KafkaItemReaderBuilder<String, Object>().partitions(0)
				.consumerProperties(kafkaOptions.consumerProperties()).partitions(0).name(topic).saveState(false)
				.topic(topic).build();
	}

}
