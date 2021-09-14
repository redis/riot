package com.redis.riot;

import com.redis.lettucemod.Utils;
import com.redis.riot.convert.RegexNamedGroupsExtractor;
import com.redis.riot.processor.CompositeItemStreamItemProcessor;
import com.redis.riot.processor.FilteringProcessor;
import com.redis.riot.processor.MapAccessor;
import com.redis.riot.processor.MapProcessor;
import com.redis.riot.processor.SpelProcessor;
import lombok.Data;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.ObjectUtils;
import picocli.CommandLine.Option;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Data
public class MapProcessorOptions {

    @Option(arity = "1..*", names = "--process", description = "SpEL expressions in the form field1=\"exp\" field2=\"exp\"...", paramLabel = "<f=exp>")
    private Map<String, Expression> spelFields;
    @Option(arity = "1..*", names = "--var", description = "Register a variable in the SpEL processor context.", paramLabel = "<v=exp>")
    private Map<String, Expression> variables;
    @Option(names = "--date", description = "Processor date format (default: ${DEFAULT-VALUE}).", paramLabel = "<string>")
    private String dateFormat = new SimpleDateFormat().toPattern();
    @Option(arity = "1..*", names = "--filter", description = "Discard records using SpEL boolean expressions.", paramLabel = "<exp>")
    private String[] filters;
    @Option(arity = "1..*", names = "--regex", description = "Extract named values from source field using regex.", paramLabel = "<f=exp>")
    private Map<String, String> regexes;

    public ItemProcessor<Map<String, Object>, Map<String, Object>> processor(RedisOptions redisOptions) throws NoSuchMethodException {
        List<ItemProcessor<Map<String, Object>, Map<String, Object>>> processors = new ArrayList<>();
        if (!ObjectUtils.isEmpty(spelFields)) {
            StandardEvaluationContext context = new StandardEvaluationContext();
            context.setVariable("date", dateFormat);
            processors.add(new SpelProcessor(redisOptions, context(), spelFields));
        }
        if (!ObjectUtils.isEmpty(regexes)) {
            Map<String, Converter<String, Map<String, String>>> fields = new LinkedHashMap<>();
            regexes.forEach((f, r) -> fields.put(f, RegexNamedGroupsExtractor.builder().regex(r).build()));
            processors.add(new MapProcessor(fields));
        }
        if (!ObjectUtils.isEmpty(filters)) {
            processors.add(new FilteringProcessor(filters));
        }
        return CompositeItemStreamItemProcessor.delegates(processors);
    }

    private EvaluationContext context() throws NoSuchMethodException {
        StandardEvaluationContext context = new StandardEvaluationContext();
        context.setVariable("date", new SimpleDateFormat(dateFormat));
        if (variables != null) {
            for (String variable : variables.keySet()) {
                context.setVariable(variable, variables.get(variable).getValue(context));
            }
        }
        context.registerFunction("geo", Utils.GeoLocation.class.getDeclaredMethod("toString", String.class, String.class));
        context.setPropertyAccessors(Collections.singletonList(new MapAccessor()));
        return context;
    }


}
