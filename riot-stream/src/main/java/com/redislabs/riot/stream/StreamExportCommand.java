package com.redislabs.riot.stream;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.RedisStreamItemReader;
import org.springframework.core.convert.converter.Converter;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import com.redislabs.riot.AbstractFlushingTransferCommand;
import com.redislabs.riot.stream.kafka.KafkaItemWriter;
import com.redislabs.riot.stream.processor.AvroProducerProcessor;
import com.redislabs.riot.stream.processor.JsonProducerProcessor;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XReadArgs.StreamOffset;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "export", description = "Import Redis streams into Kafka topics")
public class StreamExportCommand
		extends AbstractFlushingTransferCommand<StreamMessage<String, String>, ProducerRecord<String, Object>> {

	@Parameters(arity = "1..*", description = "One ore more streams to read from", paramLabel = "STREAM")
	private List<String> streams;
	@Option(names = "--block", description = "XREAD block time in millis (default: ${DEFAULT-VALUE})", hidden = true, paramLabel = "<ms>")
	private long block = 100;
	@Option(names = "--offset", description = "XREAD offset (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
	private String offset = "0-0";
	@Option(names = "--topic", description = "Target topic key (default: same as stream)", paramLabel = "<string>")
	private String topic;
	@Mixin
	private KafkaOptions kafkaOptions = new KafkaOptions();

	@Override
	protected String taskName() {
		return "Streaming from";
	}

	@Override
	protected List<ItemReader<StreamMessage<String, String>>> readers() throws Exception {
		return streams.stream().map(this::reader).collect(Collectors.toList());
	}

	private ItemReader<StreamMessage<String, String>> reader(String stream) {
		XReadArgs args = new XReadArgs();
		args.block(block);
		StreamOffset<String> offset = StreamOffset.from(stream, this.offset);
		return configure(RedisStreamItemReader.builder().args(args).offset(offset)).build();
	}

	@Override
	protected ItemProcessor<StreamMessage<String, String>, ProducerRecord<String, Object>> processor()
			throws FileNotFoundException, IOException, RestClientException {
		switch (kafkaOptions.getSerde()) {
		case JSON:
			return new JsonProducerProcessor(topicConverter());
		default:
			return new AvroProducerProcessor(topicConverter());
		}
	}

	private Converter<StreamMessage<String, String>, String> topicConverter() {
		if (topic == null) {
			return StreamMessage::getStream;
		}
		return m -> topic;
	}

	@Override
	protected ItemWriter<ProducerRecord<String, Object>> writer() throws Exception {
		return KafkaItemWriter.<String>builder()
				.kafkaTemplate(
						new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(kafkaOptions.producerProperties())))
				.build();
	}

}
