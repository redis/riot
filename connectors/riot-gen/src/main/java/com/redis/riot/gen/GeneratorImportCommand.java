package com.redis.riot.gen;

import com.redis.lettucemod.Utils;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.search.Field;
import com.redis.lettucemod.api.search.IndexInfo;
import com.redis.lettucemod.api.sync.RediSearchCommands;
import com.redis.riot.AbstractImportCommand;
import com.redis.riot.MapProcessorOptions;
import com.redis.riot.RiotStepBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
@Command(name = "import", description = "Import generated data using the Spring Expression Language (SpEL)")
public class GeneratorImportCommand extends AbstractImportCommand<Map<String, Object>, Map<String, Object>> {

    @CommandLine.Mixin
    private GenerateOptions options = new GenerateOptions();
    @CommandLine.ArgGroup(exclusive = false, heading = "Processor options%n")
    private MapProcessorOptions processorOptions = new MapProcessorOptions();

    @Override
    protected Flow flow(StepBuilderFactory stepBuilderFactory) throws Exception {
        StepBuilder stepBuilder = stepBuilderFactory.get("generate-step");
        return flow(step(stepBuilder, "Generating", reader()).build());
    }

    private ItemReader<Map<String, Object>> reader() {
        log.debug("Creating Faker reader with {}", options);
        FakerItemReader reader = FakerItemReader.builder().generator(generator()).start(options.getStart()).end(options.getEnd()).build();
        if (options.getSleep() > 0) {
            return new ThrottledItemReader<>(reader, options.getSleep());
        }
        return reader;
    }

    private Generator<Map<String, Object>> generator() {
        Map<String, String> fields = options.getFakerFields() == null ? new LinkedHashMap<>() : new LinkedHashMap<>(options.getFakerFields());
        if (options.getFakerIndex() != null) {
            fields.putAll(fieldsFromIndex(options.getFakerIndex()));
        }
        MapGenerator generator = MapGenerator.builder().locale(options.getLocale()).fields(fields).build();
        if (options.isIncludeMetadata()) {
            return new MapWithMetadataGenerator(generator);
        }
        return generator;
    }

    private String expression(Field field) {
        switch (field.getType()) {
            case TEXT:
                return "lorem.paragraph";
            case TAG:
                return "number.digits(10)";
            case GEO:
                return "address.longitude.concat(',').concat(address.latitude)";
            default:
                return "number.randomDouble(3,-1000,1000)";
        }
    }

    private Map<String, String> fieldsFromIndex(String index) {
        Map<String, String> fields = new LinkedHashMap<>();
        try (StatefulRedisModulesConnection<String, String> connection = getRedisOptions().connect()) {
            RediSearchCommands<String, String> commands = connection.sync();
            IndexInfo info = Utils.indexInfo(commands.indexInfo(index));
            for (Field field : info.getFields()) {
                fields.put(field.getName(), expression(field));
            }
        }
        return fields;
    }

    @Override
    protected ItemProcessor<Map<String, Object>, Map<String, Object>> processor() throws NoSuchMethodException {
        return processorOptions.processor(getRedisOptions());
    }

    @Override
    protected <I, O> RiotStepBuilder<I, O> riotStep(StepBuilder stepBuilder, String taskName) {
        RiotStepBuilder<I, O> riotStepBuilder = super.riotStep(stepBuilder, taskName);
        riotStepBuilder.initialMax(() -> options.getEnd() - options.getStart());
        return riotStepBuilder;
    }

}
