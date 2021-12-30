package com.redis.riot.stream;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import com.google.common.collect.ImmutableMap;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.riot.AbstractRiotIntegrationTests;
import com.redis.testcontainers.junit.RedisTestContext;
import com.redis.testcontainers.junit.RedisTestContextsSource;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.Range;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.StreamMessage;
import picocli.CommandLine;

@SuppressWarnings("unchecked")
class TestKafka extends AbstractRiotIntegrationTests {

	private static final Logger log = LoggerFactory.getLogger(TestKafka.class);

	@Container
	private static final KafkaContainer KAFKA = new KafkaContainer(
			DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));

	private static final long IDLE_TIMEOUT = 1000;

	@Override
	protected RiotStream app() {
		return new RiotStream();
	}

	@BeforeEach
	void clearKafka() throws InterruptedException, ExecutionException {
		Admin admin = Admin.create(kafkaConf());
		ListTopicsResult topics = admin.listTopics();
		admin.deleteTopics(topics.names().get());
	}

	private Map<String, Object> kafkaConf() {
		return ImmutableMap.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
				ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testImport(RedisTestContext redis) throws Exception {
		KafkaProducer<String, Map<String, String>> producer = new KafkaProducer<>(kafkaConf(), new StringSerializer(),
				new JsonSerializer<>());
		int count = 100;
		for (int index = 0; index < count; index++) {
			ProducerRecord<String, Map<String, String>> record = new ProducerRecord<>("topic1", map());
			Future<RecordMetadata> future = producer.send(record);
			future.get();
		}
		execute("import", redis, this::configureImportCommand);
		RedisModulesCommands<String, String> sync = redis.sync();
		List<StreamMessage<String, String>> messages = sync.xrange("topic1", Range.create("-", "+"));
		Assertions.assertEquals(count, messages.size());
		messages.forEach(m -> Assertions.assertEquals(map(), m.getBody()));
	}

	private void configureImportCommand(CommandLine.ParseResult parseResult) {
		StreamImportCommand command = parseResult.subcommand().commandSpec().commandLine().getCommand();
		command.getFlushingTransferOptions().setIdleTimeout(Duration.ofMillis(IDLE_TIMEOUT));
		configure(command.getOptions());
	}

	private void configure(KafkaOptions options) {
		options.setBrokers(new String[] { KAFKA.getBootstrapServers() });
	}

	private void configureExportCommand(CommandLine.ParseResult parseResult) {
		StreamExportCommand command = parseResult.subcommand().commandSpec().commandLine().getCommand();
		command.getFlushingTransferOptions().setIdleTimeout(Duration.ofMillis(IDLE_TIMEOUT));
		configure(command.getOptions());
	}

	private Map<String, String> map() {
		Map<String, String> map = new HashMap<>();
		map.put("field1", "value1");
		map.put("field2", "value2");
		return map;
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testExport(RedisTestContext redis) throws Exception {
		String stream = "stream1";
		int producedCount = 100;
		log.debug("Producing {} stream messages", producedCount);
		RedisModulesAsyncCommands<String, String> async = redis.async();
		async.setAutoFlushCommands(false);
		List<RedisFuture<?>> futures = new ArrayList<>();
		for (int index = 0; index < producedCount; index++) {
			futures.add(async.xadd(stream, map()));
		}
		async.flushCommands();
		LettuceFutures.awaitAll(Duration.ofSeconds(1), futures.toArray(new RedisFuture[0]));
		async.setAutoFlushCommands(true);
		execute("export", redis, this::configureExportCommand);
		KafkaConsumer<String, Map<String, Object>> consumer = new KafkaConsumer<>(
				ImmutableMap.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
						ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString(),
						ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
				new StringDeserializer(), new JsonDeserializer<>());
		consumer.subscribe(Collections.singletonList(stream));
		List<ConsumerRecord<String, Map<String, Object>>> consumerRecords = new ArrayList<>();
		while (consumerRecords.size() < producedCount) {
			ConsumerRecords<String, Map<String, Object>> records = consumer.poll(Duration.ofMillis(600));
			records.forEach(consumerRecords::add);
		}
		Assertions.assertEquals(producedCount, consumerRecords.size());
		consumerRecords.forEach(r -> Assertions.assertEquals(map(), r.value()));
	}

}
