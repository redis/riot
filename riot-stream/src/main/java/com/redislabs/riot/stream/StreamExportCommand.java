package com.redislabs.riot.stream;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.RedisStreamItemReader;
import org.springframework.batch.item.redis.support.ConstantConverter;
import org.springframework.batch.item.redis.support.Transfer;
import org.springframework.core.convert.converter.Converter;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import com.redislabs.riot.stream.kafka.KafkaItemWriter;
import com.redislabs.riot.stream.processor.AvroProducerProcessor;
import com.redislabs.riot.stream.processor.JsonProducerProcessor;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XReadArgs.StreamOffset;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "export", description = "Import Redis streams into Kafka topics")
public class StreamExportCommand
		extends AbstractStreamCommand<StreamMessage<String, String>, ProducerRecord<String, Object>> {

	@Parameters(arity = "1..*", description = "One ore more streams to read from", paramLabel = "STREAM")
	private List<String> streams;
	@Option(names = "--block", description = "XREAD block time in millis (default: ${DEFAULT-VALUE})", hidden = true, paramLabel = "<ms>")
	private long block = 100;
	@Option(names = "--offset", description = "XREAD offset (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
	private String offset = "0-0";
	@Option(names = "--topic", description = "Target topic key (default: same as stream)", paramLabel = "<string>")
	private String topic;

	@Override
	protected List<Transfer<StreamMessage<String, String>, ProducerRecord<String, Object>>> transfers()
			throws Exception {
		List<Transfer<StreamMessage<String, String>, ProducerRecord<String, Object>>> transfers = new ArrayList<>();
		ItemProcessor<StreamMessage<String, String>, ProducerRecord<String, Object>> processor = processor();
		KafkaItemWriter<String> writer = KafkaItemWriter.<String>builder()
				.kafkaTemplate(
						new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(kafkaOptions.producerProperties())))
				.build();
		for (String stream : streams) {
			transfers.add(transfer(reader(stream), processor, writer).build());
		}
		return transfers;
	}

	private ItemReader<StreamMessage<String, String>> reader(String stream) throws Exception {
		XReadArgs args = new XReadArgs();
		args.block(block);
		StreamOffset<String> offset = StreamOffset.from(stream, this.offset);
		return configure(RedisStreamItemReader.builder().args(args).offset(offset)).build();
	}

	private ItemProcessor<StreamMessage<String, String>, ProducerRecord<String, Object>> processor()
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
		return new ConstantConverter<>(topic);
	}

}
