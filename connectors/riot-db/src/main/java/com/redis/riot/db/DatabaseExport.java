package com.redis.riot.db;

import java.util.Map;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.lang.Nullable;

import com.redis.riot.core.AbstractExport;
import com.redis.riot.core.StepBuilder;
import com.redis.riot.core.function.KeyValueToMapFunction;
import com.redis.riot.core.function.RegexNamedGroupFunction;
import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.ValueType;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.StringCodec;

public class DatabaseExport extends AbstractExport {

    private final String sql;

    public static final boolean DEFAULT_ASSERT_UPDATES = true;

    public static final Pattern DEFAULT_KEY_PATTERN = Pattern.compile("\\w+:(?<id>.+)");

    private Pattern keyPattern = DEFAULT_KEY_PATTERN;

    private boolean assertUpdates = DEFAULT_ASSERT_UPDATES;

    private DataSourceOptions dataSourceOptions = new DataSourceOptions();

    public DatabaseExport(AbstractRedisClient client, String sql) {
        super(client);
        this.sql = sql;
    }

    public DataSourceOptions getDataSourceOptions() {
        return dataSourceOptions;
    }

    public void setDataSourceOptions(DataSourceOptions dataSourceOptions) {
        this.dataSourceOptions = dataSourceOptions;
    }

    public boolean isAssertUpdates() {
        return assertUpdates;
    }

    public void setAssertUpdates(boolean assertUpdates) {
        this.assertUpdates = assertUpdates;
    }

    public Pattern getKeyPattern() {
        return keyPattern;
    }

    public void setKeyPattern(Pattern pattern) {
        this.keyPattern = pattern;
    }

    @Override
    protected Job job() {
        StepBuilder<KeyValue<String>, Map<String, ?>> step = step(getName()).reader(reader(StringCodec.UTF8)).writer(writer());
        step.processor(processor());
        return jobBuilder().start(build(step)).build();
    }

    private JdbcBatchItemWriter<Map<String, ?>> writer() {
        DataSource dataSource = DatabaseUtils.dataSource(dataSourceOptions);
        JdbcBatchItemWriterBuilder<Map<String, ?>> builder = new JdbcBatchItemWriterBuilder<>();
        builder.itemSqlParameterSourceProvider(NullableSqlParameterSource::new);
        builder.dataSource(dataSource);
        builder.sql(sql);
        builder.assertUpdates(assertUpdates);
        JdbcBatchItemWriter<Map<String, ?>> writer = builder.build();
        writer.afterPropertiesSet();
        return writer;
    }

    private ItemProcessor<KeyValue<String>, Map<String, ?>> processor() {
        KeyValueToMapFunction function = new KeyValueToMapFunction();
        if (keyPattern != null) {
            function.setKeyFields(new RegexNamedGroupFunction(keyPattern));
        }
        return new FunctionItemProcessor<>(function);
    }

    private static class NullableSqlParameterSource extends MapSqlParameterSource {

        public NullableSqlParameterSource(@Nullable Map<String, ?> values) {
            super(values);
        }

        @Override
        @Nullable
        public Object getValue(String paramName) {
            if (!hasValue(paramName)) {
                return null;
            }
            return super.getValue(paramName);
        }

    }

    @Override
    protected ValueType getValueType() {
        return ValueType.STRUCT;
    }

}
