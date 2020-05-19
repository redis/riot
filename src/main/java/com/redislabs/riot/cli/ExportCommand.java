package com.redislabs.riot.cli;

import com.redislabs.riot.cli.db.DatabaseExportOptions;
import com.redislabs.riot.cli.file.FileExportOptions;
import com.redislabs.riot.cli.file.MapFieldExtractor;
import com.redislabs.riot.file.FlatResourceItemWriterBuilder;
import com.redislabs.riot.file.JsonResourceItemWriterBuilder;
import com.redislabs.riot.processor.KeyValueItemProcessor;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScanArgs;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.redis.RedisItemReader;
import org.springframework.batch.item.redis.RedisKeyValueItemReader;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.batch.item.support.PassThroughItemProcessor;
import org.springframework.batch.item.xml.builder.StaxEventItemWriterBuilder;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

import java.util.Arrays;
import java.util.Map;

@Command(name = "export", description = "Export data from Redis", sortOptions = false)
public class ExportCommand extends TransferCommand<KeyValue<String>, Object> {

    @Mixin
    private ExportOptions exportOptions = new ExportOptions();
    @Option(names = "--timeout", description = "Command timeout duration in seconds (default: ${DEFAULT-VALUE})", paramLabel = "<sec>")
    private long timeout = RedisURI.DEFAULT_TIMEOUT;
    @Mixin
    private MapProcessorOptions mapProcessorOptions = new MapProcessorOptions();
    @ArgGroup(exclusive = false, heading = "File options%n")
    private FileExportOptions file = new FileExportOptions();
    @ArgGroup(exclusive = false, heading = "Database options%n")
    private DatabaseExportOptions db = new DatabaseExportOptions();
    @Option(names = "--key-regex", description = "Regular expression for key-field extraction", paramLabel = "<regex>")
    private String keyRegex;

    @Override
    protected ItemReader<KeyValue<String>> reader() {
        ScanArgs scanArgs = ScanArgs.Builder.limit(exportOptions.getScanCount()).match(exportOptions.getScanMatch());
        RedisItemReader.Options readerOptions = RedisItemReader.Options.builder().batchSize(exportOptions.getBatchSize()).threads(exportOptions.getThreads()).queueCapacity(exportOptions.getQueueCapacity()).build();
        return RedisKeyValueItemReader.builder().redisOptions(redisOptions()).options(readerOptions).options(readerOptions).scanArgs(scanArgs).build();
    }

    @Override
    protected ItemProcessor<KeyValue<String>, Object> processor() {
        if (db.isSet()) {
            return (ItemProcessor) KeyValueItemProcessor.builder().build();
        }
        if (file.isSet()) {
            switch (file.getFileType()) {
                case DELIMITED:
                case FIXED:
                    return (ItemProcessor) KeyValueItemProcessor.builder().build();
                case JSON:
                case XML:
                    return (ItemProcessor) new PassThroughItemProcessor<>();
            }
        }
        throw new IllegalArgumentException("Unknown export target");
    }

    @Override
    protected ItemWriter<Object> writer() throws Exception {
        if (db.isSet()) {
            JdbcBatchItemWriterBuilder<Map<String, Object>> builder = new JdbcBatchItemWriterBuilder<Map<String, Object>>();
            builder.itemSqlParameterSourceProvider(MapSqlParameterSource::new);
            builder.dataSource(db.getDataSource());
            builder.sql(db.getSql());
            builder.assertUpdates(!db.isNoAssertUpdates());
            JdbcBatchItemWriter<Map<String, Object>> writer = builder.build();
            writer.afterPropertiesSet();
            return (ItemWriter) writer;
        }
        if (file.isSet()) {
            WritableResource resource = file.getResource();
            switch (file.getFileType()) {
                case DELIMITED:
                    return (ItemWriter) delimitedWriter(resource);
                case FIXED:
                    return (ItemWriter) formattedWriter(resource);
                case JSON:
                    return (ItemWriter) jsonWriter(resource);
                case XML:
                    return (ItemWriter) xmlWriter(resource);
            }
        }
        throw new IllegalArgumentException("Unknown export target");
    }

    @Override
    protected String taskName() {
        return "Exporting";
    }

    private FlatResourceItemWriterBuilder<Map<String, String>> flatWriterBuilder(Resource resource, String headerLine) {
        FlatResourceItemWriterBuilder<Map<String, String>> builder = new FlatResourceItemWriterBuilder<>();
        builder.append(file.isAppend());
        builder.encoding(file.getEncoding());
        builder.lineSeparator(file.getLineSeparator());
        builder.resource(resource);
        builder.saveState(false);
        if (headerLine != null) {
            builder.headerCallback(writer -> writer.write(headerLine));
        }
        return builder;
    }

    private AbstractItemStreamItemWriter<KeyValue<String>> jsonWriter(Resource resource) {
        JsonResourceItemWriterBuilder<KeyValue<String>> builder = new JsonResourceItemWriterBuilder<>();
        builder.name("json-writer");
        builder.append(file.isAppend());
        builder.encoding(file.getEncoding());
        builder.jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>());
        builder.lineSeparator(file.getLineSeparator());
        builder.resource(resource);
        builder.saveState(false);
        return builder.build();
    }

    private AbstractItemStreamItemWriter<KeyValue<String>> xmlWriter(Resource resource) {
        StaxEventItemWriterBuilder<KeyValue<String>> builder = new StaxEventItemWriterBuilder<>();
        builder.name("xml-writer");
        builder.encoding(file.getEncoding());
        builder.forceSync(file.isForceSync());
        Jaxb2Marshaller marshaller = new Jaxb2Marshaller();
        marshaller.setClassesToBeBound(KeyValue.class);
        builder.marshaller(marshaller);
        builder.rootTagName(file.getRoot());
        builder.resource(resource);
        builder.saveState(false);
        return builder.build();
    }


    private AbstractItemStreamItemWriter<Map<String, String>> delimitedWriter(Resource resource) {
        String headerLine = null;
        if (file.isHeader()) {
            headerLine = String.join(file.getDelimiter(), file.getNames());
        }
        FlatResourceItemWriterBuilder<Map<String, String>> builder = flatWriterBuilder(resource, headerLine);
        builder.name("delimited-writer");
        FlatResourceItemWriterBuilder.DelimitedBuilder<Map<String, String>> delimited = builder.delimited();
        delimited.delimiter(file.getDelimiter());
        delimited.fieldExtractor(MapFieldExtractor.builder().names(file.getNames()).build());
        if (file.getNames().length > 0) {
            delimited.names(file.getNames());
        }
        return builder.build();
    }

    private AbstractItemStreamItemWriter<Map<String, String>> formattedWriter(Resource resource) {
        String headerLine = null;
        if (file.isHeader()) {
            headerLine = String.format(file.getLocale(), file.getFormat(), Arrays.asList(file.getNames()).toArray());
        }
        FlatResourceItemWriterBuilder<Map<String, String>> builder = flatWriterBuilder(resource, headerLine);
        FlatResourceItemWriterBuilder.FormattedBuilder<Map<String, String>> formatted = builder.formatted();
        builder.name("formatted-writer");
        formatted.fieldExtractor(MapFieldExtractor.builder().names(file.getNames()).build());
        if (file.getNames().length > 0) {
            formatted.names(file.getNames());
        }
        formatted.format(file.getFormat());
        formatted.locale(file.getLocale());
        if (file.getMinLength() != null) {
            formatted.minimumLength(file.getMinLength());
        }
        if (file.getMaxLength() != null) {
            formatted.maximumLength(file.getMaxLength());
        }
        return builder.build();
    }

}
