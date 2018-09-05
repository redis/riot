package com.redislabs.recharge.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;
import com.redislabs.recharge.RechargeConfiguration.IndexConfiguration;
import com.redislabs.recharge.redis.index.AbstractIndexWriter;
import com.redislabs.recharge.redis.index.GeoIndexWriter;
import com.redislabs.recharge.redis.index.ListIndexWriter;
import com.redislabs.recharge.redis.index.SearchIndexWriter;
import com.redislabs.recharge.redis.index.SetIndexWriter;
import com.redislabs.recharge.redis.index.SuggestionIndexWriter;
import com.redislabs.recharge.redis.index.ZSetIndexWriter;

@Component
public class RedisLoadConfiguration {

	@Autowired
	private StringRedisTemplate template;
	@Autowired
	private RediSearchClient rediSearchClient;

	public AbstractEntityWriter getEntityWriter(EntityConfiguration entity) {
		switch (entity.getCommand().getType()) {
		case Nil:
			return new NilWriter(template, entity);
		case Set:
			return new StringWriter(template, entity, getObjectWriter(entity));
		case Hincrby:
			return new HincrbyWriter(template, entity);
		default:
			return new HashWriter(template, entity);
		}
	}

	private ObjectWriter getObjectWriter(EntityConfiguration entity) {
		switch (entity.getFormat()) {
		case Xml:
			return new XmlMapper().writer().withRootName(entity.getName());
		default:
			break;
		}
		return new ObjectMapper().writer();
	}

	public AbstractIndexWriter getIndexWriter(EntityConfiguration entity, IndexConfiguration index) {
		if (index.getName() == null) {
			index.setName(getKeyspace(entity, index));
		}
		if (index.getFields() == null || index.getFields().length == 0) {
			index.setFields(entity.getKeys());
		}
		switch (index.getType()) {
		case Geo:
			return new GeoIndexWriter(template, entity, index);
		case List:
			return new ListIndexWriter(template, entity, index);
		case Search:
			return new SearchIndexWriter(template, entity, index, rediSearchClient);
		case Suggestion:
			return new SuggestionIndexWriter(template, entity, index, rediSearchClient);
		case Zset:
			return new ZSetIndexWriter(template, entity, index);
		default:
			return new SetIndexWriter(template, entity, index);
		}
	}

	private String getKeyspace(EntityConfiguration entity, IndexConfiguration index) {
		String suffix = getSuffix(entity, index);
		switch (index.getType()) {
		case Search:
			return entity.getName() + "Idx" + suffix;
		case Suggestion:
			return entity.getName() + "Suggestion" + suffix;
		default:
			return String.join(AbstractRedisWriter.KEY_SEPARATOR, entity.getName(), "index" + suffix);
		}
	}

	private String getSuffix(EntityConfiguration entity, IndexConfiguration index) {
		if (entity.getIndexes().size() == 0) {
			return "";
		}
		return String.valueOf(entity.getIndexes().indexOf(index) + 1);
	}

	public ItemStreamWriter<Map<String, Object>> getWriter(EntityConfiguration entity) {
		AbstractEntityWriter writer = getEntityWriter(entity);
		if (entity.getIndexes().size() > 0) {
			List<ItemWriter<? super Map<String, Object>>> writers = new ArrayList<>();
			writers.add(writer);
			for (IndexConfiguration index : entity.getIndexes()) {
				writers.add(getIndexWriter(entity, index));
			}
			CompositeItemWriter<Map<String, Object>> composite = new CompositeItemWriter<>();
			composite.setDelegates(writers);
			return composite;
		}
		return writer;
	}

}
