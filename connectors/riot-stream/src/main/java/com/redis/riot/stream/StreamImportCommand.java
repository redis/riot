package com.redis.riot.stream;

import com.redis.riot.AbstractFlushingTransferCommand;
import com.redis.riot.RedisOptions;
import com.redis.riot.RiotStepBuilder;
import com.redis.riot.redis.FilteringOptions;
import com.redis.riot.stream.kafka.KafkaItemReader;
import com.redis.riot.stream.kafka.KafkaItemReaderBuilder;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.codec.StringCodec;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.OperationItemWriter;
import org.springframework.batch.item.redis.support.operation.Xadd;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
@Command(name = "import", description = "Import Kafka topics into Redis streams")
public class StreamImportCommand extends AbstractFlushingTransferCommand {

    @SuppressWarnings("unused")
    @Parameters(arity = "0..*", description = "One ore more topics to read from", paramLabel = "TOPIC")
    private String[] topics;
    @CommandLine.Mixin
    private KafkaOptions options = new KafkaOptions();
    @SuppressWarnings("unused")
    @Option(names = "--key", description = "Target stream key (default: same as topic)", paramLabel = "<string>")
    private String key;
    @SuppressWarnings("unused")
    @Option(names = "--maxlen", description = "Stream maxlen", paramLabel = "<int>")
    private Long maxlen;
    @SuppressWarnings("unused")
    @Option(names = "--trim", description = "Stream efficient trimming ('~' flag)")
    private boolean approximateTrimming;
    @CommandLine.Mixin
    private FilteringOptions filteringOptions = new FilteringOptions();

    @Override
    protected Flow flow(StepBuilderFactory stepBuilderFactory) {
        Assert.isTrue(!ObjectUtils.isEmpty(topics), "No topic specified");
        List<Step> steps = new ArrayList<>();
        Properties consumerProperties = options.consumerProperties();
        log.debug("Using Kafka consumer properties: {}", consumerProperties);
        for (String topic : topics) {
            log.debug("Creating Kafka reader for topic {}", topic);
            KafkaItemReader<String, Object> reader = new KafkaItemReaderBuilder<String, Object>().partitions(0).consumerProperties(consumerProperties).partitions(0).name(topic).saveState(false).topic(topic).build();
            StepBuilder stepBuilder = stepBuilderFactory.get(topic + "-stream-import-step");
            RiotStepBuilder<ConsumerRecord<String, Object>, ConsumerRecord<String, Object>> step = riotStep(stepBuilder, "Importing from " + topic);
            steps.add(step.reader(reader).writer(writer()).flushingOptions(flushingTransferOptions).build().build());
        }
        return flow(steps.toArray(new Step[0]));
    }

    private ItemWriter<ConsumerRecord<String, Object>> writer() {
        OperationItemWriter.RedisOperation<String, String, ConsumerRecord<String, Object>> operation = new Xadd<>(keyConverter(), bodyConverter(), xAddArgs());
        RedisOptions redisOptions = getRedisOptions();
        if (redisOptions.isCluster()) {
            log.debug("Creating cluster stream writer");
            return OperationItemWriter.operation(operation).codec(StringCodec.UTF8).client(redisOptions.redisClusterClient()).poolConfig(redisOptions.poolConfig()).build();
        }
        log.debug("Creating stream writer");
        return OperationItemWriter.operation(operation).codec(StringCodec.UTF8).client(redisOptions.redisClient()).poolConfig(redisOptions.poolConfig()).build();
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
