package com.redislabs.riot.stream;

import com.redislabs.riot.AbstractFlushingTransferCommand;
import com.redislabs.riot.convert.MapFlattener;
import com.redislabs.riot.convert.ObjectToStringConverter;
import com.redislabs.riot.stream.kafka.KafkaItemReader;
import com.redislabs.riot.stream.kafka.KafkaItemReaderBuilder;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.support.CommandBuilder;
import org.springframework.batch.item.redis.support.RedisClusterCommandItemWriter;
import org.springframework.batch.item.redis.support.RedisCommandItemWriter;
import org.springframework.core.convert.converter.Converter;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

@Command(name = "import", description = "Import Kafka topics into Redis streams")
public class StreamImportCommand extends AbstractFlushingTransferCommand<ConsumerRecord<String, Object>, ConsumerRecord<String, Object>> {

    @Parameters(arity = "1..*", description = "One ore more topics to read from", paramLabel = "TOPIC")
    private List<String> topics;
    @CommandLine.Mixin
    private KafkaOptions options = new KafkaOptions();
    @Option(names = "--key", description = "Target stream key (default: same as topic)", paramLabel = "<string>")
    private String key;
    @Option(names = "--maxlen", description = "Stream maxlen", paramLabel = "<int>")
    private Long maxlen;
    @Option(names = "--trim", description = "Stream efficient trimming ('~' flag)")
    private boolean approximateTrimming;

    @Override
    protected Flow flow() throws Exception {
        List<Step> steps = new ArrayList<>();
        for (String topic : topics) {
            KafkaItemReader<String, Object> reader = new KafkaItemReaderBuilder<String, Object>().partitions(0).consumerProperties(options.consumerProperties()).partitions(0).name(topic).saveState(false).topic(topic).build();
            String name = "Importing topic " + topic;
            steps.add(step(name, reader, null, writer()).build());
        }
        return flow(steps.toArray(new Step[0]));
    }

    private ItemWriter<ConsumerRecord<String, Object>> writer() {
        XAddArgs xAddArgs = xAddArgs();
        BiFunction<RedisStreamAsyncCommands<String, String>, ConsumerRecord<String, Object>, RedisFuture<?>> command = CommandBuilder.<ConsumerRecord<String, Object>>xadd().keyConverter(keyConverter()).argsConverter(r -> xAddArgs).bodyConverter(bodyConverter()).build();
        if (isCluster()) {
            return RedisClusterCommandItemWriter.builder(redisClusterPool(), command).build();
        }
        return RedisCommandItemWriter.builder(redisPool(), command).build();
    }

    private Converter<ConsumerRecord<String, Object>, Map<String, String>> bodyConverter() {
        switch (options.getSerde()) {
            case JSON:
                return new JsonToMapConverter();
            default:
                return new AvroToMapConverter();
        }
    }

    static class JsonToMapConverter implements Converter<ConsumerRecord<String, Object>, Map<String, String>> {

        private final Converter<Map<String, Object>, Map<String, String>> flattener = new MapFlattener<>(new ObjectToStringConverter());

        @Override
        @SuppressWarnings("unchecked")
        public Map<String, String> convert(ConsumerRecord<String, Object> source) {
            return flattener.convert((Map<String, Object>) source.value());
        }

    }

    static class AvroToMapConverter implements Converter<ConsumerRecord<String, Object>, Map<String, String>> {

        private final Converter<Map<String, Object>, Map<String, String>> flattener = new MapFlattener<>(new ObjectToStringConverter());

        @Override
        public Map<String, String> convert(ConsumerRecord<String, Object> source) {
            GenericRecord record = (GenericRecord) source.value();
            Map<String, Object> map = new HashMap<>();
            record.getSchema().getFields().forEach(field -> map.put(field.name(), record.get(field.name())));
            return flattener.convert(map);
        }

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
