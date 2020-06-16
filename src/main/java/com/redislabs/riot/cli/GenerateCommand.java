package com.redislabs.riot.cli;

import com.redislabs.lettuce.helper.RedisOptions;
import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.RediSearchUtils;
import com.redislabs.lettusearch.index.IndexInfo;
import com.redislabs.lettusearch.index.field.Field;
import com.redislabs.lettusearch.index.field.GeoField;
import com.redislabs.lettusearch.index.field.TagField;
import com.redislabs.lettusearch.index.field.TextField;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.faker.FakerItemReader;
import picocli.CommandLine;

import java.util.*;

@Slf4j
@CommandLine.Command(name = "gen", description = "Import generated data", subcommands = FakerHelpCommand.class)
public class GenerateCommand extends AbstractImportCommand<Map<String, Object>> {

    @CommandLine.Parameters(description = "SpEL expressions", paramLabel = "SPEL")
    private Map<String, String> fakerFields = new LinkedHashMap<>();
    @CommandLine.Option(names = "--faker-index", description = "Use given search index to introspect Faker fields", paramLabel = "<index>")
    private String fakerIndex;
    @CommandLine.Option(names = "--locale", description = "Faker locale (default: ${DEFAULT-VALUE})", paramLabel = "<tag>")
    private Locale locale = Locale.ENGLISH;
    @CommandLine.Option(names = "--metadata", description = "Include metadata (index, partition)")
    private boolean includeMetadata;

    @Override
    protected ItemReader<Map<String, Object>> reader() {
        return FakerItemReader.builder().locale(locale).includeMetadata(includeMetadata).fields(fakerFields()).build();
    }

    private String expression(Field field) {
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

    private String quotes(String field, String expression) {
        return "\"" + field + "=" + expression + "\"";
    }


    private List<String> fakerArgs(Map<String, String> fakerFields) {
        List<String> args = new ArrayList<>();
        fakerFields.forEach((k, v) -> args.add(quotes(k, v)));
        return args;
    }

    private Map<String, String> fakerFields() {
        Map<String, String> fields = new LinkedHashMap<>(fakerFields);
        if (fakerIndex == null) {
            return fields;
        }
        RedisOptions redisOptions = redisOptions();
        RediSearchClient rediSearchClient = redisOptions.getClientResources() == null ? RediSearchClient.create(redisOptions.getRedisURI()) : RediSearchClient.create(redisOptions.getClientResources(), redisOptions.getRedisURI());
        RediSearchCommands<String, String> commands = rediSearchClient.connect().sync();
        IndexInfo info = RediSearchUtils.getInfo(commands.ftInfo(fakerIndex));
        for (Field field : info.getFields()) {
            fields.put(field.getName(), expression(field));
        }
        log.info("Introspected fields: {}", String.join(" ", fakerArgs(fields)));
        return fields;
    }

    @Override
    protected ItemProcessor<Map<String, Object>, Object> processor() {
        return getParentCommand().objectMapProcessor();
    }
}
