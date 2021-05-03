package com.redislabs.riot.stream;

import com.google.common.collect.ImmutableMap;
import com.redislabs.riot.AbstractTaskCommand;
import com.redislabs.riot.FlushingTransferOptions;
import com.redislabs.riot.RiotIntegrationTest;
import com.redislabs.testcontainers.RedisContainer;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.Range;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import io.lettuce.core.api.sync.RedisStreamCommands;
import lombok.extern.slf4j.Slf4j;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import picocli.CommandLine;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Future;

@SuppressWarnings("unchecked")
@Slf4j
public class TestKafka extends RiotIntegrationTest {

    @Override
    protected RiotStream app() {
        return new RiotStream();
    }

    @Container
    private final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka"));

    @ParameterizedTest
    @MethodSource("containers")
    public void testImport(RedisContainer container) throws Exception {
        KafkaProducer<String, Map<String, String>> producer = new KafkaProducer<>(ImmutableMap.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(), ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()), new StringSerializer(), new JsonSerializer<>());
        int count = 100;
        for (int index = 0; index < count; index++) {
            ProducerRecord<String, Map<String, String>> record = new ProducerRecord<>("topic1", map());
            Future<RecordMetadata> future = producer.send(record);
            future.get();
        }
        execute("import", container, this::configureImportCommand);
        RedisStreamCommands<String, String> sync = sync(container);
        List<StreamMessage<String, String>> messages = sync.xrange("topic1", Range.create("-", "+"));
        Assertions.assertEquals(count, messages.size());
        messages.forEach(m -> Assertions.assertEquals(map(), m.getBody()));
    }

    private void configureImportCommand(CommandLine.ParseResult parseResult) {
        StreamImportCommand command = parseResult.subcommand().commandSpec().commandLine().getCommand();
        command.getFlushingTransferOptions().setIdleTimeout(Duration.ofMillis(500));
        configure(command.getOptions());
    }

    private void configure(KafkaOptions options) {
        options.setBrokers(new String[]{KAFKA.getBootstrapServers()});
    }

    private void configureExportCommand(CommandLine.ParseResult parseResult) {
        StreamExportCommand command = parseResult.subcommand().commandSpec().commandLine().getCommand();
        command.getFlushingTransferOptions().setIdleTimeout(Duration.ofMillis(500));
        configure(command.getOptions());
    }

    private Map<String, String> map() {
        Map<String, String> map = new HashMap<>();
        map.put("field1", "value1");
        map.put("field2", "value2");
        return map;
    }

    @ParameterizedTest
    @MethodSource("containers")
    public void testExport(RedisContainer container) throws Exception {
        String stream = "stream1";
        int producedCount = 100;
        log.info("Producing {} stream messages", producedCount);
        BaseRedisAsyncCommands<String, String> async = async(container);
        async.setAutoFlushCommands(false);
        List<RedisFuture<?>> futures = new ArrayList<>();
        for (int index = 0; index < producedCount; index++) {
            futures.add(((RedisStreamAsyncCommands<String, String>) async).xadd(stream, map()));
        }
        async.flushCommands();
        LettuceFutures.awaitAll(Duration.ofSeconds(1), futures.toArray(new RedisFuture[0]));
        async.setAutoFlushCommands(true);
        execute("export", container, this::configureExportCommand);
        KafkaConsumer<String, Map<String, Object>> consumer = new KafkaConsumer<>(ImmutableMap.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(), ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString(), ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"), new StringDeserializer(), new JsonDeserializer<>());
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
