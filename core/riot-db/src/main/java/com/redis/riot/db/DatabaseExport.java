package com.redis.riot.db;

import java.util.Map;

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
import com.redis.riot.core.function.RegexNamedGroupExtractor;
import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.ValueType;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.StringCodec;

public class DatabaseExport extends AbstractExport {

    private final String sql;

    public static final boolean DEFAULT_ASSERT_UPDATES = true;

    public static final String DEFAULT_KEY_REGEX = "\\w+:(?<id>.+)";

    private String keyRegex = DEFAULT_KEY_REGEX;

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

    public String getKeyRegex() {
        return keyRegex;
    }

    public void setKeyRegex(String keyRegex) {
        this.keyRegex = keyRegex;
    }

    @Override
    protected Job job() {
        StepBuilder<KeyValue<String>, Map<String, Object>> step = step(getName()).reader(reader(StringCodec.UTF8))
                .writer(writer());
        step.processor(processor());
        return jobBuilder().start(build(step)).build();
    }

    private JdbcBatchItemWriter<Map<String, Object>> writer() {
        DataSource dataSource = DatabaseUtils.dataSource(dataSourceOptions);
        JdbcBatchItemWriterBuilder<Map<String, Object>> builder = new JdbcBatchItemWriterBuilder<>();
        builder.itemSqlParameterSourceProvider(NullableSqlParameterSource::new);
        builder.dataSource(dataSource);
        builder.sql(sql);
        builder.assertUpdates(assertUpdates);
        JdbcBatchItemWriter<Map<String, Object>> writer = builder.build();
        writer.afterPropertiesSet();
        return writer;
    }

    private ItemProcessor<KeyValue<String>, Map<String, Object>> processor() {
        RegexNamedGroupExtractor keyFieldExtractor = new RegexNamedGroupExtractor(keyRegex);
        return new FunctionItemProcessor<>(new KeyValueToMapFunction(keyFieldExtractor));
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
