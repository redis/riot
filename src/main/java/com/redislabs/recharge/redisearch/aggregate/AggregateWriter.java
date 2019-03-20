package com.redislabs.recharge.redisearch.aggregate;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.aggregate.AggregateOptions;
import com.redislabs.lettusearch.aggregate.AggregateOptions.AggregateOptionsBuilder;
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
import com.redislabs.recharge.MapTemplate;
import com.redislabs.recharge.redisearch.RediSearchCommandWriter;
import com.redislabs.recharge.redisearch.aggregate.operation.ApplyOperation;
import com.redislabs.recharge.redisearch.aggregate.operation.FilterOperation;
import com.redislabs.recharge.redisearch.aggregate.operation.GroupOperation;
import com.redislabs.recharge.redisearch.aggregate.operation.LimitOperation;
import com.redislabs.recharge.redisearch.aggregate.operation.ReduceFunction;
import com.redislabs.recharge.redisearch.aggregate.operation.SortOperation;

import io.lettuce.core.RedisFuture;
import lombok.extern.slf4j.Slf4j;

@SuppressWarnings({ "rawtypes", "unchecked" })
@Slf4j
public class AggregateWriter extends RediSearchCommandWriter<AggregateConfiguration> {

	private MapTemplate template = new MapTemplate();

	public AggregateWriter(AggregateConfiguration config) {
		super(config);
	}

	@Override
	protected RedisFuture<?> write(Map record, RediSearchAsyncCommands<String, String> commands) {
		String query = query(record);
		AggregateOptions options = getAggregateOptions();
		log.debug("Aggregate index={}, query='{}' options={}", config.getIndex(), query, options);
		return commands.aggregate(config.getIndex(), query, options);
	}

	private String query(Map record) {
		return template.expand(config.getQuery(), record);
	}

	private AggregateOptions getAggregateOptions() {
		AggregateOptionsBuilder builder = AggregateOptions.builder();
		builder.verbatim(config.isVerbatim());
		builder.withSchema(config.isWithSchema());
		builder.loads(config.getLoads());
		config.getOperations().forEach(operation -> builder.operation(operation(operation)));
		return builder.build();
	}

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
