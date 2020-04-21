package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.lettusearch.search.Document;
import com.redislabs.lettusearch.suggest.Suggestion;
import com.redislabs.riot.redis.RedisCommands;
import com.redislabs.riot.redis.writer.KeyBuilder;
import com.redislabs.riot.redis.writer.RediSearchCommandWriter;

import lombok.Setter;

@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class AbstractFtSugadd extends AbstractKeyMapCommandWriter
        implements RediSearchCommandWriter<Map<String, Object>> {

    private @Setter
    String field;
    private @Setter
    boolean increment;
    private @Setter
    String score;
    private @Setter
    double defaultScore = 1d;

    protected AbstractFtSugadd(KeyBuilder keyBuilder, boolean keepKeyFields, String field, String score,
                               double defaultScore, boolean increment) {
        super(keyBuilder, keepKeyFields);
        this.field = field;
        this.score = score;
        this.defaultScore = defaultScore;
        this.increment = increment;
    }

    @Override
    protected Object write(RedisCommands commands, Object redis, String key, Map<String, Object> item) {
        String string = convert(item.get(field), String.class);
        if (string == null) {
            return null;
        }
        Suggestion<String> suggestion = Suggestion.<String>builder().string(string).score(convert(item.getOrDefault(this.score, defaultScore), Double.class)).payload(payload(item)).build();
        return commands.sugadd(redis, key, suggestion, increment);
    }

    protected abstract String payload(Map<String, Object> item);

}
