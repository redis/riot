package com.redislabs.riot.stream;

import com.redislabs.riot.AbstractFlushingTransferCommand;
import com.redislabs.riot.RedisOptions;
import com.redislabs.riot.RiotStepBuilder;
import com.redislabs.riot.stream.kafka.KafkaItemWriter;
import com.redislabs.riot.stream.processor.AvroProducerProcessor;
import com.redislabs.riot.stream.processor.JsonProducerProcessor;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs.StreamOffset;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.redis.StreamItemReader;
import org.springframework.core.convert.converter.Converter;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
@Command(name = "export", description = "Import Redis streams into Kafka topics")
public class StreamExportCommand extends AbstractFlushingTransferCommand {

    @SuppressWarnings("unused")
    @Parameters(arity = "0..*", description = "One ore more streams to read from", paramLabel = "STREAM")
    private String[] streams;
    @CommandLine.Mixin
    private KafkaOptions options = KafkaOptions.builder().build();
    @Option(names = "--offset", description = "XREAD offset (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
    private String offset = "0-0";
    @SuppressWarnings("unused")
    @Option(names = "--topic", description = "Target topic key (default: same as stream)", paramLabel = "<string>")
    private String topic;

    @Override
    protected Flow flow(StepBuilderFactory stepBuilderFactory) {
        Assert.isTrue(!ObjectUtils.isEmpty(streams), "No stream specified");
        List<Step> steps = new ArrayList<>();
        for (String stream : streams) {
            StreamItemReader<String, String> reader = reader(StreamOffset.from(stream, offset));
            StepBuilder stepBuilder = stepBuilderFactory.get(stream + "-stream-export-step");
            RiotStepBuilder<StreamMessage<String, String>, ProducerRecord<String, Object>> step = riotStep(stepBuilder, "Exporting from " + stream);
            steps.add(configure(step.reader(reader).processor(processor()).writer(writer()).build()).build());
        }
        return flow(steps.toArray(new Step[0]));
    }

    private StreamItemReader<String, String> reader(StreamOffset<String> offset) {
        RedisOptions redisOptions = getRedisOptions();
        if (redisOptions.isCluster()) {
            log.debug("Creating cluster stream reader with offset {}", offset);
            return StreamItemReader.client(redisOptions.redisClusterClient()).offset(offset).build();
        }
        log.debug("Creating stream reader with offset {}", offset);
        return StreamItemReader.client(redisOptions.redisClient()).offset(offset).build();
    }

    private KafkaItemWriter<String> writer() {
        Map<String, Object> producerProperties = options.producerProperties();
        log.debug("Creating Kafka writer with producer properties {}", producerProperties);
        return KafkaItemWriter.<String>builder().kafkaTemplate(new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProperties))).build();
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
