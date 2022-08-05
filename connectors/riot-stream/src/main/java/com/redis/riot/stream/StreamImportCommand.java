package com.redis.riot.stream;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemWriter;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.redis.riot.AbstractTransferCommand;
import com.redis.riot.FlushingTransferOptions;
import com.redis.riot.RedisWriterOptions;
import com.redis.riot.RiotStep;
import com.redis.riot.redis.FilteringOptions;
import com.redis.riot.stream.kafka.KafkaItemReader;
import com.redis.riot.stream.kafka.KafkaItemReaderBuilder;
import com.redis.spring.batch.writer.operation.Xadd;
import com.redis.spring.batch.writer.operation.Xadd.XaddBuilder;

import io.lettuce.core.XAddArgs;
import io.lettuce.core.codec.StringCodec;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "import", description = "Import Kafka topics into Redis streams")
public class StreamImportCommand extends AbstractTransferCommand {

	private static final Logger log = Logger.getLogger(StreamImportCommand.class.getName());

	private static final String NAME = "stream-import";
	@Mixin
	private FlushingTransferOptions flushingTransferOptions = new FlushingTransferOptions();
	@Parameters(arity = "0..*", description = "One ore more topics to read from", paramLabel = "TOPIC")
	private List<String> topics;
	@Mixin
	private KafkaOptions options = new KafkaOptions();
	@Option(names = "--key", description = "Target stream key (default: same as topic)", paramLabel = "<string>")
	private Optional<String> key = Optional.empty();
	@Option(names = "--maxlen", description = "Stream maxlen", paramLabel = "<int>")
	private Optional<Long> maxlen = Optional.empty();
	@Option(names = "--trim", description = "Stream efficient trimming ('~' flag)")
	private boolean approximateTrimming;
	@Mixin
	private FilteringOptions filteringOptions = new FilteringOptions();
	@ArgGroup(exclusive = false, heading = "Writer options%n")
	private RedisWriterOptions writerOptions = new RedisWriterOptions();

	public FlushingTransferOptions getFlushingTransferOptions() {
		return flushingTransferOptions;
	}

	public void setFlushingTransferOptions(FlushingTransferOptions flushingTransferOptions) {
		this.flushingTransferOptions = flushingTransferOptions;
	}

	public List<String> getTopics() {
		return topics;
	}

	public void setTopics(List<String> topics) {
		this.topics = topics;
	}

	public KafkaOptions getOptions() {
		return options;
	}

	public void setOptions(KafkaOptions options) {
		this.options = options;
	}

	public void setKey(String key) {
		this.key = Optional.of(key);
	}

	public void setMaxlen(long maxlen) {
		this.maxlen = Optional.of(maxlen);
	}

	public boolean isApproximateTrimming() {
		return approximateTrimming;
	}

	public void setApproximateTrimming(boolean approximateTrimming) {
		this.approximateTrimming = approximateTrimming;
	}

	public FilteringOptions getFilteringOptions() {
		return filteringOptions;
	}

	public void setFilteringOptions(FilteringOptions filteringOptions) {
		this.filteringOptions = filteringOptions;
	}

	public RedisWriterOptions getWriterOptions() {
		return writerOptions;
	}

	public void setWriterOptions(RedisWriterOptions writerOptions) {
		this.writerOptions = writerOptions;
	}

	@Override
	protected Job job(JobBuilder jobBuilder) throws Exception {
		Assert.isTrue(!ObjectUtils.isEmpty(topics), "No topic specified");
		Iterator<String> topicIterator = topics.iterator();
		SimpleJobBuilder simpleJobBuilder = jobBuilder.start(topicImportStep(topicIterator.next()));
		while (topicIterator.hasNext()) {
			simpleJobBuilder.next(topicImportStep(topicIterator.next()));
		}
		return simpleJobBuilder.build();
	}

	private TaskletStep topicImportStep(String topic) throws Exception {
		Properties consumerProperties = options.consumerProperties();
		log.log(Level.FINE, "Creating Kafka reader for topic {0} with {1}", new Object[] { topic, consumerProperties });
		KafkaItemReader<String, Object> reader = new KafkaItemReaderBuilder<String, Object>().partitions(0)
				.consumerProperties(consumerProperties).partitions(0).name(topic).saveState(false).topic(topic).build();
		return flushingTransferOptions.configure(step(RiotStep.reader(reader).writer(writer()).name(topic + "-" + NAME)
				.taskName("Importing from " + topic).build())).build();
	}

	private ItemWriter<ConsumerRecord<String, Object>> writer() {
		XaddBuilder<String, String, ConsumerRecord<String, Object>> xadd = Xadd
				.<String, String, ConsumerRecord<String, Object>>key(keyConverter()).body(bodyConverter());
		xAddArgs().ifPresent(xadd::args);
		return writerOptions.configureWriter(writer(getRedisOptions(), StringCodec.UTF8).operation(xadd.build()))
				.build();
	}

	private Converter<ConsumerRecord<String, Object>, Map<String, String>> bodyConverter() {
		if (options.getSerde() == KafkaOptions.SerDe.JSON) {
			return new JsonToMapConverter(filteringOptions.converter());
		}
		return new AvroToMapConverter(filteringOptions.converter());
	}

	private Converter<ConsumerRecord<String, Object>, String> keyConverter() {
		if (key.isPresent()) {
			return s -> key.get();
		}
		return ConsumerRecord::topic;
	}

	private Optional<XAddArgs> xAddArgs() {
		if (maxlen.isEmpty()) {
			return Optional.empty();
		}
		XAddArgs args = new XAddArgs();
		args.maxlen(maxlen.get());
		args.approximateTrimming(approximateTrimming);
		return Optional.of(args);
	}

}
