package com.redislabs.riot.stream;

import com.google.common.collect.ImmutableMap;
import com.redislabs.riot.RiotApp;
import com.redislabs.riot.test.AbstractStandaloneRedisTest;
import io.lettuce.core.Range;
import io.lettuce.core.StreamMessage;
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
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobExecution;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Future;

@Testcontainers
public class TestKafka extends AbstractStandaloneRedisTest {

    @Container
    private static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.2.1"));

    @Override
    protected RiotApp app() {
        return new RiotStream();
    }

    @Override
    protected String appName() {
        return "riot-stream";
    }

    @Override
    protected String process(String command) {
        return super.process(command).replace("localhost:9092", kafka.getBootstrapServers());
    }

    @Test
    public void testImport() throws Exception {
        KafkaProducer<String, Map<String, String>> producer = new KafkaProducer<>(ImmutableMap.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(), ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()), new StringSerializer(), new JsonSerializer<Map<String, String>>());
        int count = 100;
        for (int index = 0; index < count; index++) {
            ProducerRecord<String, Map<String, String>> record = new ProducerRecord<String, Map<String, String>>("topic1", map());
            Future<RecordMetadata> future = producer.send(record);
            future.get();
        }
        StreamImportCommand command = (StreamImportCommand) command("/import.txt");
        command.run();
        Thread.sleep(200);
        List<StreamMessage<String, String>> messages = sync.xrange("topic1", Range.create("-", "+"));
        Assertions.assertEquals(count, messages.size());
        messages.forEach(m -> Assertions.assertEquals(map(), m.getBody()));
    }

    private Map<String, String> map() {
        Map<String, String> map = new HashMap<>();
        map.put("field1", "value1");
        map.put("field2", "value2");
        return map;
    }

    @Test
    public void testExport() throws Exception {
        StreamExportCommand command = (StreamExportCommand) command("/export.txt");
        JobExecution execution = command.executeAsync();
        while (!execution.isRunning()) {
            Thread.sleep(10);
        }
        String stream = "stream1";
        int producedCount = 100;
        for (int index = 0; index < producedCount; index++) {
            sync.xadd(stream, map());
        }
        KafkaConsumer<String, Map<String, Object>> consumer = new KafkaConsumer<>(ImmutableMap.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(), ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString(), ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"), new StringDeserializer(), new JsonDeserializer<>());
        consumer.subscribe(Collections.singletonList(stream));
        List<ConsumerRecord<String, Map<String, Object>>> consumerRecords = new ArrayList<>();
        while (consumerRecords.size() < 100) {
            ConsumerRecords<String, Map<String, Object>> records = consumer.poll(Duration.ofSeconds(30));
            records.forEach(consumerRecords::add);
        }
        Assertions.assertEquals(producedCount, consumerRecords.size());
        consumerRecords.forEach(r -> Assertions.assertEquals(map(), r.value()));
        execution.stop();
        command.shutdown();
    }

}
