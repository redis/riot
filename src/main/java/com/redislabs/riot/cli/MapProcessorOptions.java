package com.redislabs.riot.cli;

import com.redislabs.riot.convert.MapProcessor;
import com.redislabs.riot.convert.map.RegexNamedGroupsExtractor;
import com.redislabs.riot.processor.SpelProcessor;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.batch.item.support.ScriptItemProcessor;
import org.springframework.batch.item.support.builder.ScriptItemProcessorBuilder;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.io.FileSystemResource;
import picocli.CommandLine.Option;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MapProcessorOptions {

    @Option(arity = "1..*", names = "--regex", description = "Extract named values from source field using regex", paramLabel = "<field=exp>")
    private Map<String, String> regexes;
    @Option(arity = "1..*", names = "--spel", description = "SpEL expression to produce a field", paramLabel = "<field=exp>")
    private Map<String, String> spel;
    @Option(arity = "1..*", names = "--spel-var", description = "Register a variable in the SpEL processor context", paramLabel = "<v=exp>")
    private Map<String, String> variables;
    @Option(names = "--date-format", description = "Processor date format (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
    private String dateFormat = new SimpleDateFormat().toPattern();
    @Option(names = "--script", description = "Use an inline script to process items", paramLabel = "<script>")
    private String script;
    @Option(names = "--script-file", description = "Use an external script to process items", paramLabel = "<file>")
    private File scriptFile;
    @Option(names = "--script-lang", description = "Script language (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
    private String processorScriptLanguage = "ECMAScript";

    public ScriptItemProcessor<Map<String, Object>, Map<String, Object>> scriptProcessor() throws Exception {
        System.setProperty("nashorn.args", "--no-deprecation-warning");
        ScriptItemProcessorBuilder<Map<String, Object>, Map<String, Object>> builder = new ScriptItemProcessorBuilder<>();
        builder.language(processorScriptLanguage);
        if (script != null) {
            builder.scriptSource(script);
        }
        if (scriptFile != null) {
            builder.scriptResource(new FileSystemResource(scriptFile));
        }
        ScriptItemProcessor<Map<String, Object>, Map<String, Object>> processor = builder.build();
        processor.afterPropertiesSet();
        return processor;
    }

    public void addField(String name, String expression) {
        spel.put(name, expression);
    }

    public ItemProcessor<Map<String, Object>, Map<String, Object>> processor() throws Exception {
        List<ItemProcessor<Map<String, Object>, Map<String, Object>>> processors = new ArrayList<>();
        if (regexes != null) {
            Map<String, Converter<String, Map<String, String>>> extractors = new LinkedHashMap<>();
            for (Map.Entry<String, String> fieldRegex : regexes.entrySet()) {
                extractors.put(fieldRegex.getKey(), RegexNamedGroupsExtractor.builder().regex(fieldRegex.getValue()).build());
            }
            MapProcessor mapProcessor = MapProcessor.<String, String>builder().extractors(extractors).build();
            processors.add(mapProcessor);
        }
        if (spel != null) {
            processors.add(SpelProcessor.builder().dateFormat(new SimpleDateFormat(dateFormat)).variables(variables).fields(spel).build());
        }
        if (script == null && scriptFile == null) {
            processors.add(scriptProcessor());
        }
        if (processors.isEmpty()) {
            return null;
        }
        if (processors.size() == 1) {
            return processors.get(0);
        }
        CompositeItemProcessor<Map<String, Object>, Map<String, Object>> composite = new CompositeItemProcessor<>();
        composite.setDelegates(processors);
        return composite;
    }

}
