package com.redislabs.riot.cli;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.RediSearchUtils;
import com.redislabs.lettusearch.index.IndexInfo;
import com.redislabs.lettusearch.index.field.Field;
import com.redislabs.lettusearch.index.field.GeoField;
import com.redislabs.lettusearch.index.field.TagField;
import com.redislabs.lettusearch.index.field.TextField;
import com.redislabs.riot.generator.GeneratorReader;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.RedisOptions;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.*;

@Slf4j
@Command(name = "gen", description = "Generate random data in Redis", subcommands = FakerHelpCommand.class, sortOptions = false)
public class GeneratorCommand extends ImportCommand {

    @Option(names = "--faker", arity = "1..*", description = "SpEL expression to generate a field", paramLabel = "<name=SpEL>")
    private Map<String, String> fakerFields = new LinkedHashMap<>();
    @Option(names = "--faker-help", description = "Show all available Faker properties")
    private boolean fakerHelp;
    @Option(names = "--faker-index", description = "Use given search index to introspect Faker fields", paramLabel = "<index>")
    private String fakerIndex;
    @Option(names = {"-d",
            "--data"}, arity = "0..*", description = "Field sizes in bytes", paramLabel = "<field=size>")
    private Map<String, Integer> simpleFields = new LinkedHashMap<>();
    @Option(names = "--locale", description = "Faker locale (default: ${DEFAULT-VALUE})", paramLabel = "<tag>")
    private Locale locale = Locale.ENGLISH;
    @Option(names = "--metadata", description = "Include metadata (index, partition)")
    private boolean includeMetadata;

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

    @Override
    protected ItemReader<Map<String, ?>> reader() {
        return (ItemReader) GeneratorReader.builder().locale(locale).includeMetadata(includeMetadata).fakerFields(fakerFields()).simpleFields(simpleFields).build();
    }

    private Map<String, String> fakerFields() {
        Map<String, String> fields = new LinkedHashMap<>(fakerFields);
        if (fakerIndex != null) {
            RedisOptions redisOptions = redisOptions();
            RediSearchClient rediSearchClient = redisOptions.getClientResources() == null ? RediSearchClient.create(redisOptions.getRedisURI()) : RediSearchClient.create(redisOptions.getClientResources(), redisOptions.getRedisURI());
            RediSearchCommands<String, String> commands = rediSearchClient.connect().sync();
            IndexInfo info = RediSearchUtils.getInfo(commands.ftInfo(fakerIndex));
            for (Field field : info.getFields()) {
                fields.put(field.getName(), expression(field));
            }
            log.info("Introspected fields: {}", String.join(" ", fakerArgs(fields)));
        }
        return fields;
    }

    @Override
    protected String taskName() {
        return "Generating";
    }

}
