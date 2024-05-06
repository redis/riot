package com.redis.riot.db;

import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;

import com.redis.riot.core.AbstractExport;
import com.redis.riot.core.KeyValueMapProcessorOptions;
import com.redis.riot.core.RiotUtils;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.reader.MemKeyValue;

import io.lettuce.core.codec.StringCodec;

public class DatabaseExport extends AbstractExport {

	public static final boolean DEFAULT_ASSERT_UPDATES = true;

	private String sql;
	private DataSourceOptions dataSourceOptions = new DataSourceOptions();
	private boolean assertUpdates = DEFAULT_ASSERT_UPDATES;
	private KeyValueMapProcessorOptions mapProcessorOptions = new KeyValueMapProcessorOptions();

	@Override
	protected Job job() {
		RedisItemReader<String, String, MemKeyValue<String, Object>> reader = RedisItemReader.struct();
		configure(reader);
		return jobBuilder().start(step(getName(), reader, writer()).processor(processor()).build()).build();
	}

	private ItemProcessor<MemKeyValue<String, Object>, Map<String, Object>> processor() {
		return RiotUtils.processor(processor(StringCodec.UTF8), mapProcessorOptions.processor());
	}

	private JdbcBatchItemWriter<Map<String, Object>> writer() {
		JdbcBatchItemWriterBuilder<Map<String, Object>> builder = new JdbcBatchItemWriterBuilder<>();
		builder.itemSqlParameterSourceProvider(NullableSqlParameterSource::new);
		builder.dataSource(dataSourceOptions.dataSource());
		builder.sql(sql);
		builder.assertUpdates(assertUpdates);
		JdbcBatchItemWriter<Map<String, Object>> writer = builder.build();
		writer.afterPropertiesSet();
		return writer;
	}

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

	public KeyValueMapProcessorOptions getMapProcessorOptions() {
		return mapProcessorOptions;
	}

	public void setMapProcessorOptions(KeyValueMapProcessorOptions options) {
		this.mapProcessorOptions = options;
	}

}
