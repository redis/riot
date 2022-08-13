package com.redis.riot.stream;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.batch.core.Job;
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
import com.redis.riot.JobCommandContext;
import com.redis.riot.RiotStep;
import com.redis.riot.stream.kafka.KafkaItemWriter;
import com.redis.riot.stream.processor.AvroProducerProcessor;
import com.redis.riot.stream.processor.JsonProducerProcessor;
import com.redis.spring.batch.RedisItemReader;

import io.lettuce.core.StreamMessage;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "export", description = "Import Redis streams into Kafka topics")
public class StreamExportCommand extends AbstractTransferCommand {

	private static final Logger log = Logger.getLogger(StreamExportCommand.class.getName());

	private static final String NAME = "stream-export";
	@Mixin
	private FlushingTransferOptions flushingTransferOptions = new FlushingTransferOptions();
	@Parameters(arity = "0..*", description = "One ore more streams to read from", paramLabel = "STREAM")
	private List<String> streams;
	@Mixin
	private KafkaOptions options = new KafkaOptions();
	@Option(names = "--offset", description = "XREAD offset (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
	private String offset = "0-0";
	@Option(names = "--topic", description = "Target topic key (default: same as stream)", paramLabel = "<string>")
	private Optional<String> topic = Optional.empty();

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

	public void setTopic(String topic) {
		this.topic = Optional.of(topic);
	}

	@Override
	protected Job createJob(JobCommandContext context) throws Exception {
		Assert.isTrue(!ObjectUtils.isEmpty(streams), "No stream specified");
		Iterator<String> streamIterator = streams.iterator();
		SimpleJobBuilder simpleJobBuilder = context.getJobRunner().job(NAME)
				.start(streamExportStep(context, streamIterator.next()));
		while (streamIterator.hasNext()) {
			simpleJobBuilder.next(streamExportStep(context, streamIterator.next()));
		}
		return simpleJobBuilder.build();
	}

	private TaskletStep streamExportStep(JobCommandContext context, String stream) {
		return flushingTransferOptions
				.configure(step(context.getJobRunner().step(stream + "-" + NAME),
						RiotStep.reader(RedisItemReader.stream(context.getRedisClient(), stream).build())
								.writer(writer()).processor(processor()).taskName("Exporting from " + stream).build()))
				.build();
	}

	private KafkaItemWriter<String> writer() {
		Map<String, Object> producerProperties = options.producerProperties();
		log.log(Level.FINE, "Creating Kafka writer with producer properties {0}", producerProperties);
		return new KafkaItemWriter<>(new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProperties)));
	}

	private ItemProcessor<StreamMessage<String, String>, ProducerRecord<String, Object>> processor() {
		if (options.getSerde() == KafkaOptions.SerDe.JSON) {
			return new JsonProducerProcessor(topicConverter());
		}
		return new AvroProducerProcessor(topicConverter());
	}

	private Converter<StreamMessage<String, String>, String> topicConverter() {
		if (topic.isPresent()) {
			return s -> topic.get();
		}
		return StreamMessage::getStream;
	}

}
