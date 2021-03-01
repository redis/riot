package com.redislabs.riot.gen;

import com.redislabs.lettusearch.*;
import com.redislabs.riot.AbstractImportCommand;
import com.redislabs.riot.KeyValueProcessingOptions;
import io.lettuce.core.RedisURI;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.item.ItemProcessor;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

@Command(name = "import", description = "Import generated data")
public class GenerateCommand extends AbstractImportCommand<Map<String, Object>, Map<String, Object>> {

    @Parameters(description = "SpEL expressions", paramLabel = "SPEL")
    private Map<String, String> fakerFields = new LinkedHashMap<>();
    @Option(names = "--introspect", description = "Use given search index to introspect Faker fields", paramLabel = "<index>")
    private String fakerIndex;
    @Option(names = "--locale", description = "Faker locale (default: ${DEFAULT-VALUE})", paramLabel = "<tag>")
    private Locale locale = Locale.ENGLISH;
    @Option(names = "--metadata", description = "Include metadata (index, partition)")
    private boolean includeMetadata;
    @Option(names = "--start", description = "Start index (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private long start = 0;
    @Option(names = "--end", description = "End index (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private long end = 1000;
    @Option(names = "--sleep", description = "Duration in ms to sleep before each item generation (default: ${DEFAULT-VALUE})", paramLabel = "<ms>")
    private long sleep = 0;
    @CommandLine.Mixin
    private KeyValueProcessingOptions processingOptions = new KeyValueProcessingOptions();

    @Override
    protected Flow flow() {
        FakerItemReader reader = FakerItemReader.builder().locale(locale).includeMetadata(includeMetadata).fields(fakerFields(getRedisURI())).start(start).end(end).sleep(sleep).build();
        return flow(step("Generating", reader).build());
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

    private Map<String, String> fakerFields(RedisURI uri) {
        Map<String, String> fields = new LinkedHashMap<>(fakerFields);
        if (fakerIndex == null) {
            return fields;
        }
        RediSearchClient client = RediSearchClient.create(uri);
        try (StatefulRediSearchConnection<String, String> connection = client.connect()) {
            RediSearchCommands<String, String> commands = connection.sync();
            IndexInfo<String> info = RediSearchUtils.getInfo(commands.ftInfo(fakerIndex));
            for (Field<String> field : info.getFields()) {
                fields.put(field.getName(), expression(field));
            }
            return fields;
        } finally {
            client.shutdown();
            client.getResources().shutdown();
        }
    }

    @Override
    protected ItemProcessor<Map<String, Object>, Map<String, Object>> processor() {
        return processingOptions.processor(connection);
    }
}
