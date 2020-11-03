package com.redislabs.riot.gen;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.RediSearchUtils;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.index.IndexInfo;
import com.redislabs.lettusearch.index.field.Field;
import com.redislabs.lettusearch.index.field.GeoField;
import com.redislabs.lettusearch.index.field.TagField;
import com.redislabs.lettusearch.index.field.TextField;
import com.redislabs.riot.AbstractImportCommand;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "import", aliases = { "i" }, description = "Import generated data")
public class GenerateCommand extends AbstractImportCommand<Map<String, Object>, Map<String, Object>> {

    @Parameters(description = "SpEL expressions", paramLabel = "SPEL")
    private Map<String, String> fakerFields = new LinkedHashMap<>();

    @Option(names = "--faker-index", description = "Use given search index to introspect Faker fields", paramLabel = "<index>")
    private String fakerIndex;

    @Option(names = "--locale", description = "Faker locale (default: ${DEFAULT-VALUE})", paramLabel = "<tag>")
    private Locale locale = Locale.ENGLISH;

    @Option(names = "--metadata", description = "Include metadata (index, partition)")
    private boolean includeMetadata;

    @Override
    protected List<ItemReader<Map<String, Object>>> readers() throws Exception {
        return Collections.singletonList(
                FakerItemReader.builder().locale(locale).includeMetadata(includeMetadata).fields(fakerFields()).build());
    }

    @Override
    protected ItemProcessor<Map<String, Object>, Map<String, Object>> processor() throws Exception {
        return mapProcessor();
    }

    private String expression(Field<String> field) {
        if (field instanceof TextField) {
            return "lorem.paragraph";
        }
        if (field instanceof TagField) {
            return "number.digits(10)";
        }
        if (field instanceof GeoField) {
            return "address.longitude.concat(',').concat(address.latitude)";
        }
        return "number.randomDouble(3,-1000,1000)";
    }

    private Map<String, String> fakerFields() {
        Map<String, String> fields = new LinkedHashMap<>(fakerFields);
        if (fakerIndex == null) {
            return fields;
        }
        RediSearchClient client = RediSearchClient.create(getRedisConnectionOptions().redisURI());
        StatefulRediSearchConnection<String, String> connection = client.connect();
        RediSearchCommands<String, String> commands = connection.sync();
        IndexInfo<String> info = RediSearchUtils.getInfo(commands.ftInfo(fakerIndex));
        for (Field<String> field : info.getFields()) {
            fields.put(field.getName(), expression(field));
        }
        return fields;
    }

}
