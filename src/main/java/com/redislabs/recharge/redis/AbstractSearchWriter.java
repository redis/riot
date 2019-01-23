package com.redislabs.recharge.redis;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.search.DropOptions;
import com.redislabs.lettusearch.search.Schema;
import com.redislabs.lettusearch.search.Schema.SchemaBuilder;
import com.redislabs.lettusearch.search.field.Field;
import com.redislabs.lettusearch.search.field.GeoField;
import com.redislabs.lettusearch.search.field.NumericField;
import com.redislabs.lettusearch.search.field.TextField;
import com.redislabs.lettusearch.search.field.TextField.TextFieldBuilder;
import com.redislabs.recharge.RechargeConfiguration.RediSearchField;
import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;
import com.redislabs.recharge.RechargeConfiguration.SearchConfiguration;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractSearchWriter extends AbstractRedisWriter {

	public AbstractSearchWriter(RediSearchClient connection, RedisWriterConfiguration writer) {
		super(connection, writer);
	}

	@Override
	protected void doOpen() {
		SearchConfiguration search = config.getSearch();
		if (!search.getSchema().isEmpty()) {
			if (search.isDrop()) {
				try {
					commands.drop(config.getKeyspace(), DropOptions.builder().build());
				} catch (Exception e) {
					log.debug("Could not drop index {}", config.getKeyspace(), e);
				}
			}
			if (search.isCreate()) {
				SchemaBuilder builder = Schema.builder();
				search.getSchema().forEach(entry -> builder.field(getField(entry)));
				Schema schema = builder.build();
				try {
					commands.create(config.getKeyspace(), schema);
				} catch (Exception e) {
					if (e.getMessage().startsWith("Index already exists")) {
						log.debug("Ignored failure to create index {}", config.getKeyspace(), e);
					} else {
						log.error("Could not create index {}", config.getKeyspace(), e);
					}
				}
			}
		}
	}

	private Field getField(RediSearchField field) {
		switch (field.getType()) {
		case Geo:
			return GeoField.builder().name(field.getName()).sortable(field.isSortable()).build();
		case Numeric:
			return NumericField.builder().name(field.getName()).sortable(field.isSortable()).build();
		default:
			TextFieldBuilder builder = TextField.builder().name(field.getName()).sortable(field.isSortable())
					.noStem(field.isNoStem());
			if (field.getWeight() != null) {
				builder.weight(field.getWeight());
			}
			if (field.getMatcher() != null) {
				builder.matcher(field.getMatcher());
			}
			return builder.build();
		}
	}

}
