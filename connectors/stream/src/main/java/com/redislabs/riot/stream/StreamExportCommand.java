package com.redislabs.riot.stream;

import com.redislabs.riot.AbstractTransferCommand;
import com.redislabs.riot.FlushingTransferOptions;
import com.redislabs.riot.StepBuilder;
import com.redislabs.riot.stream.kafka.KafkaItemWriter;
import com.redislabs.riot.stream.processor.AvroProducerProcessor;
import com.redislabs.riot.stream.processor.JsonProducerProcessor;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.redis.StreamItemReader;
import org.springframework.core.convert.converter.Converter;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.ArrayList;
import java.util.List;

@Command(name = "export", description = "Import Redis streams into Kafka topics")
public class StreamExportCommand extends AbstractTransferCommand<StreamMessage<String, String>, ProducerRecord<String, Object>> {

    @Parameters(arity = "1..*", description = "One ore more streams to read from", paramLabel = "STREAM")
    private List<String> streams;
    @CommandLine.Mixin
    private KafkaOptions options = new KafkaOptions();
    @CommandLine.Mixin
    private FlushingTransferOptions flushingOptions = FlushingTransferOptions.builder().build();
    @Option(names = "--block", description = "XREAD block time in millis (default: ${DEFAULT-VALUE})", hidden = true, paramLabel = "<ms>")
    private long block = 100;
    @Option(names = "--offset", description = "XREAD offset (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
    private String offset = "0-0";
    @Option(names = "--topic", description = "Target topic key (default: same as stream)", paramLabel = "<string>")
    private String topic;

    @Override
    protected Flow flow() {
        List<Step> steps = new ArrayList<>();
        for (String stream : streams) {
            StepBuilder<StreamMessage<String, String>, ProducerRecord<String, Object>> step = stepBuilder("Exporting stream " + stream);
            steps.add(flushingOptions.configure(step.reader(reader(StreamOffset.from(stream, offset))).processor(processor()).writer(writer()).build()).build());
        }
        return flow(steps.toArray(new Step[0]));
    }

    private StreamItemReader<String, String> reader(StreamOffset<String> offset) {
        if (isCluster()) {
            return StreamItemReader.builder((StatefulRedisClusterConnection<String, String>) connection).offset(offset).build();
        }
        return StreamItemReader.builder((StatefulRedisConnection<String, String>) connection).offset(offset).build();
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
