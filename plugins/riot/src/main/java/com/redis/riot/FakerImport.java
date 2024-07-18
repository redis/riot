package com.redis.riot;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.util.StringUtils;

import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.search.IndexInfo;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.riot.core.Expression;
import com.redis.riot.faker.FakerItemReader;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "faker", description = "Import Faker data.")
public class FakerImport extends AbstractImportCommand {

	public static final int DEFAULT_COUNT = 1000;
	public static final Locale DEFAULT_LOCALE = Locale.ENGLISH;

	@Parameters(arity = "1..*", description = "SpEL expressions in the form field1=\"exp\" field2=\"exp\"...", paramLabel = "EXPRESSION")
	private Map<String, Expression> fields = new LinkedHashMap<>();

	@Option(names = "--count", description = "Number of items to generate (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int count = DEFAULT_COUNT;

	@Option(names = "--infer", description = "Introspect given RediSearch index to infer Faker fields.", paramLabel = "<name>")
	private String searchIndex;

	@Option(names = "--locale", description = "Faker locale (default: ${DEFAULT-VALUE}).", paramLabel = "<tag>")
	private Locale locale = DEFAULT_LOCALE;

	@Override
	protected Job job() {
		return job(step(reader()).maxItemCount(count));
	}

	private FakerItemReader reader() {
		FakerItemReader reader = new FakerItemReader();
		reader.setMaxItemCount(count);
		reader.setLocale(locale);
		reader.setFields(fields());
		return reader;
	}

	private Map<String, Expression> fields() {
		Map<String, Expression> allFields = new LinkedHashMap<>(fields);
		if (StringUtils.hasLength(searchIndex)) {
			Map<String, Expression> searchFields = new LinkedHashMap<>();
			IndexInfo info = RedisModulesUtils.indexInfo(connection.sync().ftInfo(searchIndex));
			for (Field<String> field : info.getFields()) {
				searchFields.put(field.getName(), Expression.parse(expression(field)));
			}
			allFields.putAll(searchFields);
		}
		return allFields;
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
		this.locale = locale;
	}

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

}
