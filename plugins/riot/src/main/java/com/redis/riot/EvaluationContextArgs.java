package com.redis.riot;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.CollectionUtils;

import com.redis.lettucemod.search.GeoLocation;
import com.redis.riot.core.Expression;
import com.redis.riot.core.RiotUtils;

import lombok.ToString;
import net.datafaker.Faker;
import picocli.CommandLine.Option;

@ToString
public class EvaluationContextArgs {

	public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
	public static final String DEFAULT_NUMBER_FORMAT = "#,###.##";
	public static final String VAR_DATE = "date";
	public static final String VAR_NUMBER = "number";
	public static final String VAR_FAKER = "faker";

	@Option(arity = "1..*", names = "--var", description = "SpEL expressions for context variables, in the form var=\"exp\". For details see https://docs.spring.io/spring-framework/reference/core/expressions.html", paramLabel = "<v=exp>")
	private Map<String, Expression> varExpressions = new LinkedHashMap<>();

	@Option(names = "--date-format", description = "Date/time format (default: ${DEFAULT-VALUE}). For details see https://www.baeldung.com/java-simple-date-format#date_time_patterns", paramLabel = "<fmt>")
	private String dateFormat = DEFAULT_DATE_FORMAT;

	@Option(names = "--number-format", description = "Number format (default: ${DEFAULT-VALUE}). For details see https://www.baeldung.com/java-decimalformat", paramLabel = "<fmt>")
	private String numberFormat = DEFAULT_NUMBER_FORMAT;

	private Map<String, Object> vars = new LinkedHashMap<>();

	public StandardEvaluationContext evaluationContext() {
		StandardEvaluationContext context = new StandardEvaluationContext();
		RiotUtils.registerFunction(context, "geo", GeoLocation.class, "toString", String.class, String.class);
		context.setVariable(VAR_DATE, new SimpleDateFormat(dateFormat));
		context.setVariable(VAR_NUMBER, new DecimalFormat(numberFormat));
		context.setVariable(VAR_FAKER, new Faker());
		if (!CollectionUtils.isEmpty(vars)) {
			vars.forEach(context::setVariable);
		}
		if (!CollectionUtils.isEmpty(varExpressions)) {
			varExpressions.forEach((k, v) -> context.setVariable(k, v.getValue(context)));
		}
		return context;
	}

	public String getDateFormat() {
		return dateFormat;
	}

	public void setDateFormat(String format) {
		this.dateFormat = format;
	}

	public Map<String, Expression> getVarExpressions() {
		return varExpressions;
	}

	public void setVarExpressions(Map<String, Expression> expressions) {
		this.varExpressions = expressions;
	}

	public Map<String, Object> getVars() {
		return vars;
	}

	public void setVars(Map<String, Object> variables) {
		this.vars = variables;
	}

	public String getNumberFormat() {
		return numberFormat;
	}

	public void setNumberFormat(String numberFormat) {
		this.numberFormat = numberFormat;
	}

}
