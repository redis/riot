package com.redis.riot.faker;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.expression.Expression;
import org.springframework.util.Assert;

import com.redis.lettucemod.api.sync.RediSearchCommands;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.search.IndexInfo;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.riot.core.AbstractMapImport;
import com.redis.riot.core.RiotContext;
import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.StepBuilder;
import com.redis.spring.batch.common.Range;

public class FakerImport extends AbstractMapImport {

    public static final int DEFAULT_COUNT = 1000;

    public static final Locale DEFAULT_LOCALE = Locale.ENGLISH;

    public static final Range DEFAULT_INDEX_RANGE = Range.from(1);

    private Map<String, Expression> fields = new LinkedHashMap<>();

    private int count = DEFAULT_COUNT;

    private String searchIndex;

    private Locale locale = DEFAULT_LOCALE;

    public Map<String, Expression> getFields() {
        return fields;
    }

    public void setFields(Map<String, Expression> fields) {
        this.fields = fields;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getSearchIndex() {
        return searchIndex;
    }

    public void setSearchIndex(String index) {
        this.searchIndex = index;
    }

    public Locale getLocale() {
        return locale;
    }

    public void setLocale(Locale locale) {
        Assert.notNull(locale, "Locale must not be null");
        this.locale = locale;
    }

    @Override
    protected Job job(RiotContext executionContext) {
        StepBuilder<Map<String, Object>, Map<String, Object>> step = createStep();
        step.name(getName());
        step.reader(reader(executionContext));
        step.writer(writer(executionContext));
        return jobBuilder().start(step.build()).build();
    }

    private FakerItemReader reader(RiotContext executionContext) {
        FakerItemReader reader = new FakerItemReader();
        reader.setMaxItemCount(count);
        reader.setLocale(locale);
        reader.setFields(fields(executionContext));
        return reader;
    }

    private Map<String, Expression> fields(RiotContext executionContext) {
        Map<String, Expression> allFields = new LinkedHashMap<>(fields);
        if (searchIndex != null) {
            allFields.putAll(searchIndexFields(executionContext));
        }
        return allFields;
    }

    private Map<String, Expression> searchIndexFields(RiotContext executionContext) {
        Map<String, Expression> searchFields = new LinkedHashMap<>();
        RediSearchCommands<String, String> commands = executionContext.getRedisContext().getConnection().sync();
        IndexInfo info = RedisModulesUtils.indexInfo(commands.ftInfo(searchIndex));
        for (Field<String> field : info.getFields()) {
            searchFields.put(field.getName(), RiotUtils.parse(expression(field)));
        }
        return searchFields;
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

}
