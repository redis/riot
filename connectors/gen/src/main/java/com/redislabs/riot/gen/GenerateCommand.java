package com.redislabs.riot.gen;

import com.redislabs.lettusearch.*;
import com.redislabs.riot.AbstractImportCommand;
import com.redislabs.riot.ProcessorOptions;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.item.ItemProcessor;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
@Command(name = "import", description = "Import generated data")
public class GenerateCommand extends AbstractImportCommand<Map<String, Object>, Map<String, Object>> {

    @CommandLine.Mixin
    private GenerateOptions options = GenerateOptions.builder().build();
    @CommandLine.Mixin
    private ProcessorOptions processingOptions = ProcessorOptions.builder().build();

    @Override
    protected Flow flow() throws Exception {
        log.info("Creating Faker reader with {}", options);
        Map<String, String> fields = options.getFakerFields() == null ? new LinkedHashMap<>() : new LinkedHashMap<>(options.getFakerFields());
        if (options.getFakerIndex() != null) {
            fields.putAll(fieldsFromIndex(options.getFakerIndex()));
        }
        FakerItemReader reader = FakerItemReader.builder().locale(options.getLocale()).includeMetadata(options.isIncludeMetadata()).fields(fields).start(options.getStart()).end(options.getEnd()).sleep(options.getSleep()).build();
        return flow(step("generate-step", "Generating", reader).build());
    }

    private String expression(Field<String> field) {
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
        RediSearchClient client = RediSearchClient.create(getRedisURI());
        try (StatefulRediSearchConnection<String, String> connection = client.connect()) {
            RediSearchCommands<String, String> commands = connection.sync();
            IndexInfo<String> info = RediSearchUtils.getInfo(commands.ftInfo(index));
            for (Field<String> field : info.getFields()) {
                fields.put(field.getName(), expression(field));
            }
        } finally {
            client.shutdown();
            client.getResources().shutdown();
        }
        return fields;
    }

    @Override
    protected ItemProcessor<Map<String, Object>, Map<String, Object>> processor() throws NoSuchMethodException {
        return processingOptions.processor(client);
    }
}
