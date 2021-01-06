package com.redislabs.riot.stream;

import com.redislabs.riot.AbstractFlushingTransferCommand;
import com.redislabs.riot.stream.kafka.KafkaItemWriter;
import com.redislabs.riot.stream.processor.AvroProducerProcessor;
import com.redislabs.riot.stream.processor.JsonProducerProcessor;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs.StreamOffset;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.RedisClusterStreamItemReader;
import org.springframework.batch.item.redis.RedisStreamItemReader;
import org.springframework.batch.item.redis.support.AbstractStreamItemReader;
import org.springframework.core.convert.converter.Converter;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Command(name = "export", description = "Import Redis streams into Kafka topics")
public class StreamExportCommand extends AbstractFlushingTransferCommand<StreamMessage<String, String>, ProducerRecord<String, Object>> {

    @Parameters(arity = "1..*", description = "One ore more streams to read from", paramLabel = "STREAM")
    private List<String> streams;
    @CommandLine.Mixin
    protected KafkaOptions options = new KafkaOptions();
    @Option(names = "--block", description = "XREAD block time in millis (default: ${DEFAULT-VALUE})", hidden = true, paramLabel = "<ms>")
    private long block = 100;
    @Option(names = "--offset", description = "XREAD offset (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
    private String offset = "0-0";
    @Option(names = "--topic", description = "Target topic key (default: same as stream)", paramLabel = "<string>")
    private String topic;

    @Override
    protected Flow flow() throws Exception {
        ItemProcessor<StreamMessage<String, String>, ProducerRecord<String, Object>> processor = processor();
        List<Step> steps = new ArrayList<>();
        for (String stream : streams) {
            StreamOffset<String> offset = StreamOffset.from(stream, this.offset);
            ItemReader<StreamMessage<String, String>> reader = reader(offset);
            String name = "Exporting stream " + stream;
            steps.add(step(name, reader, processor(), writer()).build());
        }
        return flow(steps.toArray(new Step[0]));
    }

    private ItemReader<StreamMessage<String, String>> reader(StreamOffset<String> offset) {
        if (isCluster()) {
            return configure(RedisClusterStreamItemReader.builder(redisClusterConnection()), offset).build();
        }
        return configure(RedisStreamItemReader.builder(redisConnection()), offset).build();
    }

    private <B extends AbstractStreamItemReader.StreamItemReaderBuilder<B>> B configure(B builder, StreamOffset<String> offset) {
        return builder.offset(offset).block(Duration.ofMillis(block));
    }

    private KafkaItemWriter<String> writer() {
        return KafkaItemWriter.<String>builder().kafkaTemplate(new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(options.producerProperties()))).build();
    }

    private ItemProcessor<StreamMessage<String, String>, ProducerRecord<String, Object>> processor() {
        switch (options.getSerde()) {
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
        return s -> topic;
    }

}
