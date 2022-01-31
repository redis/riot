package com.redis.riot.stream;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.redis.riot.AbstractTransferCommand;
import com.redis.riot.FlushingTransferOptions;
import com.redis.riot.RiotStepBuilder;
import com.redis.riot.stream.kafka.KafkaItemWriter;
import com.redis.riot.stream.processor.AvroProducerProcessor;
import com.redis.riot.stream.processor.JsonProducerProcessor;
import com.redis.spring.batch.reader.StreamItemReader;

import io.lettuce.core.StreamMessage;
import io.lettuce.core.codec.StringCodec;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "export", description = "Import Redis streams into Kafka topics")
public class StreamExportCommand extends AbstractTransferCommand {

	private static final Logger log = LoggerFactory.getLogger(StreamExportCommand.class);

	private static final String NAME = "stream-export";
	@CommandLine.Mixin
	private FlushingTransferOptions flushingTransferOptions = new FlushingTransferOptions();
	@Parameters(arity = "0..*", description = "One ore more streams to read from", paramLabel = "STREAM")
	private List<String> streams;
	@CommandLine.Mixin
	private KafkaOptions options = new KafkaOptions();
	@Option(names = "--offset", description = "XREAD offset (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
	private String offset = "0-0";
	@Option(names = "--topic", description = "Target topic key (default: same as stream)", paramLabel = "<string>")
	private String topic;

	public FlushingTransferOptions getFlushingTransferOptions() {
		return flushingTransferOptions;
	}

	public void setFlushingTransferOptions(FlushingTransferOptions flushingTransferOptions) {
		this.flushingTransferOptions = flushingTransferOptions;
	}

	public List<String> getStreams() {
		return streams;
	}

	public void setStreams(List<String> streams) {
		this.streams = streams;
	}

	public KafkaOptions getOptions() {
		return options;
	}

	public void setOptions(KafkaOptions options) {
		this.options = options;
	}

	public String getOffset() {
		return offset;
	}

	public void setOffset(String offset) {
		this.offset = offset;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	@Override
	protected Job job(JobBuilder jobBuilder) throws Exception {
		Assert.isTrue(!ObjectUtils.isEmpty(streams), "No stream specified");
		Iterator<String> streamIterator = streams.iterator();
		SimpleJobBuilder simpleJobBuilder = jobBuilder.start(streamExportStep(streamIterator.next()));
		while (streamIterator.hasNext()) {
			simpleJobBuilder.next(streamExportStep(streamIterator.next()));
		}
		return simpleJobBuilder.build();
	}

	private TaskletStep streamExportStep(String stream) throws Exception {
		StreamItemReader<String, String> reader = reader(getRedisOptions(), StringCodec.UTF8).stream(stream).build();
		RiotStepBuilder<StreamMessage<String, String>, ProducerRecord<String, Object>> step = riotStep(
				stream + "-" + NAME, "Exporting from " + stream);
		return step.reader(reader).processor(processor()).writer(writer()).flushingOptions(flushingTransferOptions)
				.build().build();
	}

	private KafkaItemWriter<String> writer() {
		Map<String, Object> producerProperties = options.producerProperties();
		log.debug("Creating Kafka writer with producer properties {}", producerProperties);
		return new KafkaItemWriter<>(new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProperties)));
	}

	private ItemProcessor<StreamMessage<String, String>, ProducerRecord<String, Object>> processor() {
		if (options.getSerde() == KafkaOptions.SerDe.JSON) {
			return new JsonProducerProcessor(topicConverter());
		}
		return new AvroProducerProcessor(topicConverter());
	}

	private Converter<StreamMessage<String, String>, String> topicConverter() {
		if (topic == null) {
			return StreamMessage::getStream;
		}
		return s -> topic;
	}

}
