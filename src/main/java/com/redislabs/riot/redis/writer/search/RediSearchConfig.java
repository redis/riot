package com.redislabs.riot.redis.writer.search;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.aggregate.Apply;
import com.redislabs.lettusearch.aggregate.Filter;
import com.redislabs.lettusearch.aggregate.Group;
import com.redislabs.lettusearch.aggregate.Group.GroupBuilder;
import com.redislabs.lettusearch.aggregate.Limit;
import com.redislabs.lettusearch.aggregate.Operation;
import com.redislabs.lettusearch.aggregate.Reducer;
import com.redislabs.lettusearch.aggregate.Sort;
import com.redislabs.lettusearch.aggregate.Sort.SortBuilder;
import com.redislabs.lettusearch.aggregate.SortProperty;
import com.redislabs.lettusearch.aggregate.reducer.Avg;
import com.redislabs.lettusearch.aggregate.reducer.By;
import com.redislabs.lettusearch.aggregate.reducer.Count;
import com.redislabs.lettusearch.aggregate.reducer.CountDistinct;
import com.redislabs.lettusearch.aggregate.reducer.FirstValue;
import com.redislabs.lettusearch.aggregate.reducer.FirstValue.FirstValueBuilder;
import com.redislabs.lettusearch.aggregate.reducer.Max;
import com.redislabs.lettusearch.aggregate.reducer.Min;
import com.redislabs.lettusearch.aggregate.reducer.Quantile;
import com.redislabs.lettusearch.aggregate.reducer.RandomSample;
import com.redislabs.lettusearch.aggregate.reducer.StdDev;
import com.redislabs.lettusearch.aggregate.reducer.Sum;
import com.redislabs.lettusearch.aggregate.reducer.ToList;
import com.redislabs.lettusearch.search.AddOptions;
import com.redislabs.lettusearch.search.Limit.LimitBuilder;
import com.redislabs.lettusearch.search.Schema;
import com.redislabs.lettusearch.search.Schema.SchemaBuilder;
import com.redislabs.lettusearch.search.SearchOptions;
import com.redislabs.lettusearch.search.SearchOptions.SearchOptionsBuilder;
import com.redislabs.lettusearch.search.SortBy;
import com.redislabs.lettusearch.search.field.Field;
import com.redislabs.lettusearch.search.field.GeoField;
import com.redislabs.lettusearch.search.field.NumericField;
import com.redislabs.lettusearch.search.field.TagField;
import com.redislabs.lettusearch.search.field.TextField;
import com.redislabs.riot.redis.writer.search.aggregate.ApplyOperation;
import com.redislabs.riot.redis.writer.search.aggregate.FilterOperation;
import com.redislabs.riot.redis.writer.search.aggregate.GroupOperation;
import com.redislabs.riot.redis.writer.search.aggregate.LimitOperation;
import com.redislabs.riot.redis.writer.search.aggregate.ReduceFunction;
import com.redislabs.riot.redis.writer.search.aggregate.SortOperation;

@Configuration
@EnableConfigurationProperties(RediSearchProperties.class)
@ConditionalOnProperty("index")
public class RediSearchConfig {

	@Bean
	@StepScope
	@ConditionalOnProperty("index")
	public AbstractRediSearchWriter rediSearchWriter(
			GenericObjectPool<StatefulRediSearchConnection<String, String>> pool, RediSearchProperties redisearch) throws Exception {
		AbstractRediSearchWriter writer = writer(redisearch);
		writer.setIndex(redisearch.getIndex());
		writer.setRedisClient(null);//TODO
		return writer;
	}

	private AbstractRediSearchWriter writer(RediSearchProperties redisearch) {
		switch (redisearch.getType()) {
		case Suggest:
			return suggestWriter(redisearch);
		default:
			return addWriter(redisearch);
		}
	}

	private SuggestWriter suggestWriter(RediSearchProperties redisearch) {
		SuggestWriter writer = new SuggestWriter();
		writer.setDefaultScore(redisearch.getDefaultScore());
		writer.setField(redisearch.getField());
		writer.setIncrement(redisearch.isIncrement());
		writer.setScoreField(redisearch.getScore());
		writer.setPayloadField(redisearch.getPayload());
		return writer;
	}

	private SearchAddWriter addWriter(RediSearchProperties redisearch) {
		SearchAddWriter writer = new SearchAddWriter();
		writer.setDefaultScore(redisearch.getDefaultScore());
		writer.setDrop(redisearch.isDrop());
		writer.setDropKeepDocs(redisearch.isDropKeepDocs());
		writer.setKeys(redisearch.getKeys());
		writer.setOptions(AddOptions.builder().noSave(redisearch.isNoSave()).replace(redisearch.isReplace())
				.replacePartial(redisearch.isReplacePartial()).build());
		SchemaBuilder builder = Schema.builder();
		redisearch.getSchema().forEach(field -> builder.field(field(field)));
		writer.setSchema(builder.build());
		return writer;
	}

	@SuppressWarnings("unused")
	private SearchOptions searchOptions(RediSearchProperties props) {
		SearchOptionsBuilder builder = SearchOptions.builder();
		if (props.getLimit() != null) {
			LimitBuilder limitBuilder = com.redislabs.lettusearch.search.Limit.builder();
			limitBuilder.num(props.getLimit().getNum());
			limitBuilder.offset(props.getLimit().getOffset());
			builder.limit(limitBuilder.build());
		}
		builder.language(props.getLanguage());
		builder.noContent(props.isNoContent());
		builder.noStopWords(props.isNoStopWords());
		if (props.getSortBy() != null) {
			builder.sortBy(SortBy.builder().field(props.getSortBy().getField())
					.direction(props.getSortBy().getDirection()).build());
		}
		builder.verbatim(props.isVerbatim());
		builder.withPayloads(props.isWithPayloads());
		builder.withScores(props.isWithScores());
		return builder.build();
	}

	private Field field(SchemaField field) {
		switch (field.getType()) {
		case Geo:
			return GeoField.builder().name(field.getName()).sortable(field.isSortable()).noIndex(field.isNoIndex())
					.build();
		case Numeric:
			return NumericField.builder().name(field.getName()).sortable(field.isSortable()).noIndex(field.isNoIndex())
					.build();
		case Tag:
			return TagField.builder().name(field.getName()).sortable(field.isSortable()).noIndex(field.isNoIndex())
					.build();
		default:
			return TextField.builder().name(field.getName()).sortable(field.isSortable()).noIndex(field.isNoIndex())
					.matcher(field.getMatcher()).noStem(field.isNoStem()).weight(field.getWeight()).build();
		}
	}

	@SuppressWarnings("unused")
	private Operation operation(AggregateOperation operation) {
		if (operation.getApply() != null) {
			return apply(operation.getApply());
		}
		if (operation.getFilter() != null) {
			return filter(operation.getFilter());
		}
		if (operation.getLimit() != null) {
			return limit(operation.getLimit());
		}
		if (operation.getSort() != null) {
			return sort(operation.getSort());
		}
		return group(operation.getGroup());
	}

	private Sort sort(SortOperation sort) {
		SortBuilder builder = Sort.builder();
		sort.getProperties().forEach((k, v) -> builder.property(SortProperty.builder().property(k).order(v).build()));
		builder.max(sort.getMax());
		return builder.build();
	}

	private Limit limit(LimitOperation limit) {
		return Limit.builder().num(limit.getNum()).offset(limit.getOffset()).build();
	}

	private Filter filter(FilterOperation filter) {
		return Filter.builder().expression(filter.getExpression()).build();
	}

	private Apply apply(ApplyOperation apply) {
		return Apply.builder().as(apply.getAs()).expression(apply.getExpression()).build();
	}

	private Operation group(GroupOperation group) {
		GroupBuilder builder = Group.builder();
		builder.properties(group.getProperties());
		group.getReducers().forEach(reduce -> builder.reduce(reducer(reduce)));
		return builder.build();
	}

	private Reducer reducer(ReduceFunction reduce) {
		if (reduce.getAvg() != null) {
			return Avg.builder().as(reduce.getAvg().getAs()).property(reduce.getAvg().getProperty()).build();
		}
		if (reduce.getCount() != null) {
			return Count.builder().as(reduce.getCount().getAs()).build();
		}
		if (reduce.getCountDistinct() != null) {
			return CountDistinct.builder().as(reduce.getCountDistinct().getAs()).build();
		}
		if (reduce.getFirstValue() != null) {
			FirstValueBuilder builder = FirstValue.builder().as(reduce.getFirstValue().getAs())
					.property(reduce.getFirstValue().getProperty());
			if (reduce.getFirstValue().getBy() != null) {
				builder.by(By.builder().property(reduce.getFirstValue().getBy().getProperty())
						.order(reduce.getFirstValue().getBy().getOrder()).build());
			}
			return builder.build();
		}
		if (reduce.getMax() != null) {
			return Max.builder().as(reduce.getMax().getAs()).property(reduce.getMax().getProperty()).build();
		}
		if (reduce.getMin() != null) {
			return Min.builder().as(reduce.getMin().getAs()).property(reduce.getMin().getProperty()).build();
		}
		if (reduce.getQuantile() != null) {
			return Quantile.builder().as(reduce.getQuantile().getAs()).property(reduce.getQuantile().getProperty())
					.quantile(reduce.getQuantile().getQuantile()).build();
		}
		if (reduce.getRandomSample() != null) {
			return RandomSample.builder().as(reduce.getRandomSample().getAs())
					.property(reduce.getRandomSample().getProperty()).size(reduce.getRandomSample().getSize()).build();
		}
		if (reduce.getStdDev() != null) {
			return StdDev.builder().as(reduce.getStdDev().getAs()).property(reduce.getStdDev().getProperty()).build();
		}
		if (reduce.getSum() != null) {
			return Sum.builder().as(reduce.getSum().getAs()).property(reduce.getSum().getProperty()).build();
		}
		return ToList.builder().as(reduce.getToList().getAs()).property(reduce.getToList().getProperty()).build();
	}

}
