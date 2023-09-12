package com.redis.riot.db;

import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.lang.Nullable;

import com.redis.riot.core.AbstractMapExport;
import com.redis.spring.batch.ValueType;

public class DatabaseExport extends AbstractMapExport {

    public static final boolean DEFAULT_ASSERT_UPDATES = true;

    private String sql;

    private DataSourceOptions dataSourceOptions = new DataSourceOptions();

    private boolean assertUpdates = DEFAULT_ASSERT_UPDATES;

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
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

    @Override
    protected JdbcBatchItemWriter<Map<String, Object>> writer() {
        JdbcBatchItemWriterBuilder<Map<String, Object>> writer = new JdbcBatchItemWriterBuilder<>();
        writer.itemSqlParameterSourceProvider(NullableSqlParameterSource::new);
        writer.dataSource(dataSource());
        writer.sql(sql);
        writer.assertUpdates(assertUpdates);
        return writer.build();
    }

    private DataSource dataSource() {
        return DatabaseUtils.dataSource(dataSourceOptions);
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
