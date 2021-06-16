package com.redislabs.riot.redis;

import org.springframework.batch.item.redis.OperationItemWriter;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.Map;

@Command(name = "sugadd", description = "Add suggestion strings to a RediSearch auto-complete suggestion dictionary")
public class SugaddCommand extends AbstractKeyCommand {

    @Option(names = "--field", required = true, description = "Field containing the strings to add", paramLabel = "<field>")
    private String field;
    @SuppressWarnings("unused")
    @Option(names = "--score", description = "Name of the field to use for scores", paramLabel = "<field>")
    private String scoreField;
    @Option(names = "--score-default", description = "Score when field not present (default: ${DEFAULT-VALUE})", paramLabel = "<num>")
    private double scoreDefault = 1;
    @Option(names = "--payload", description = "Field containing the payload", paramLabel = "<field>")
    private String payload;
    @Option(names = "--increment", description = "Increment the existing suggestion by the score instead of replacing the score")
    private boolean increment;

    @Override
    public OperationItemWriter.RedisOperation<Map<String, Object>> operation() {
        return new Sugadd<>(key(), stringFieldExtractor(field), numberFieldExtractor(Double.class, scoreField, scoreDefault), stringFieldExtractor(payload), increment);
    }


}
