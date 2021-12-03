package com.redis.riot.stream;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.redis.riot.AbstractTransferCommand;
import com.redis.riot.FlushingTransferOptions;
import com.redis.riot.RedisWriterOptions;
import com.redis.riot.RiotStepBuilder;
import com.redis.riot.redis.FilteringOptions;
import com.redis.riot.stream.kafka.KafkaItemReader;
import com.redis.riot.stream.kafka.KafkaItemReaderBuilder;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.support.operation.Xadd;

import io.lettuce.core.XAddArgs;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "import", description = "Import Kafka topics into Redis streams")
public class StreamImportCommand extends AbstractTransferCommand {

	private static final Logger log = LoggerFactory.getLogger(StreamImportCommand.class);

	private static final String NAME = "stream-import";
	@CommandLine.Mixin
	private FlushingTransferOptions flushingTransferOptions = new FlushingTransferOptions();
	@Parameters(arity = "0..*", description = "One ore more topics to read from", paramLabel = "TOPIC")
	private String[] topics;
	@CommandLine.Mixin
	private KafkaOptions options = new KafkaOptions();
	@Option(names = "--key", description = "Target stream key (default: same as topic)", paramLabel = "<string>")
	private String key;
	@Option(names = "--maxlen", description = "Stream maxlen", paramLabel = "<int>")
	private Long maxlen;
	@Option(names = "--trim", description = "Stream efficient trimming ('~' flag)")
	private boolean approximateTrimming;
	@CommandLine.Mixin
	private FilteringOptions filteringOptions = new FilteringOptions();
	@ArgGroup(exclusive = false, heading = "Writer options%n")
	private RedisWriterOptions writerOptions = new RedisWriterOptions();

	public FlushingTransferOptions getFlushingTransferOptions() {
		return flushingTransferOptions;
	}

	public void setFlushingTransferOptions(FlushingTransferOptions flushingTransferOptions) {
		this.flushingTransferOptions = flushingTransferOptions;
	}

	public String[] getTopics() {
		return topics;
	}

	public void setTopics(String[] topics) {
		this.topics = topics;
	}

	public KafkaOptions getOptions() {
		return options;
	}

	public void setOptions(KafkaOptions options) {
		this.options = options;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public Long getMaxlen() {
		return maxlen;
	}

	public void setMaxlen(Long maxlen) {
		this.maxlen = maxlen;
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
	protected Flow flow() throws Exception {
		Assert.isTrue(!ObjectUtils.isEmpty(topics), "No topic specified");
		List<Step> steps = new ArrayList<>();
		Properties consumerProperties = options.consumerProperties();
		log.debug("Using Kafka consumer properties: {}", consumerProperties);
		for (String topic : topics) {
			log.debug("Creating Kafka reader for topic {}", topic);
			KafkaItemReader<String, Object> reader = new KafkaItemReaderBuilder<String, Object>().partitions(0)
					.consumerProperties(consumerProperties).partitions(0).name(topic).saveState(false).topic(topic)
					.build();
			RiotStepBuilder<ConsumerRecord<String, Object>, ConsumerRecord<String, Object>> step = riotStep(
					topic + "-" + NAME, "Importing from " + topic);
			Xadd<String, String, ConsumerRecord<String, Object>> xadd = Xadd
					.<String, String, ConsumerRecord<String, Object>>key(keyConverter()).body(bodyConverter())
					.args(xAddArgs()).build();
			RedisItemWriter<String, String, ConsumerRecord<String, Object>> writer = writerOptions
					.configureWriter(writer(getRedisOptions()).operation(xadd)).build();
			steps.add(step.reader(reader).writer(writer).flushingOptions(flushingTransferOptions).build().build());
		}
		return flow(NAME, steps.toArray(new Step[0]));
	}

	private Converter<ConsumerRecord<String, Object>, Map<String, String>> bodyConverter() {
		if (options.getSerde() == KafkaOptions.SerDe.JSON) {
			return new JsonToMapConverter(filteringOptions.converter());
		}
		return new AvroToMapConverter(filteringOptions.converter());
	}

	private Converter<ConsumerRecord<String, Object>, String> keyConverter() {
		if (key == null) {
			return ConsumerRecord::topic;
		}
		return s -> key;
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