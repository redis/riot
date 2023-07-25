package com.redis.riot.cli;

import java.io.IOException;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.job.builder.JobBuilderException;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.JsonObjectMarshaller;
import org.springframework.core.io.WritableResource;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redis.riot.cli.common.AbstractExportCommand;
import com.redis.riot.cli.common.CommandContext;
import com.redis.riot.cli.file.DumpOptions;
import com.redis.riot.cli.file.FileExportOptions;
import com.redis.riot.core.FileDumpType;
import com.redis.riot.core.resource.JsonResourceItemWriterBuilder;
import com.redis.riot.core.resource.XmlResourceItemWriterBuilder;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.KeyValue;

import io.lettuce.core.codec.StringCodec;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;

@Command(name = "file-export", description = "Export Redis data to JSON or XML files.")
public class FileExport extends AbstractExportCommand {

    private static final String TASK_NAME = "Exporting to file %s";

    @Parameters(arity = "1", description = "File path or URL", paramLabel = "FILE")
    private String file;

    @Mixin
    private DumpOptions dumpFileOptions = new DumpOptions();

    @ArgGroup(exclusive = false, heading = "File export options%n")
    private FileExportOptions fileExportOptions = new FileExportOptions();

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
    }

    public FileExportOptions getFileExportOptions() {
        return fileExportOptions;
    }

    public void setFileExportOptions(FileExportOptions options) {
        this.fileExportOptions = options;
    }

    @Override
    protected Job job(CommandContext context) {
        WritableResource resource;
        try {
            resource = fileExportOptions.outputResource(file);
        } catch (IOException e) {
            throw new JobBuilderException(e);
        }
        ItemWriter<KeyValue<String>> writer = writer(resource);
        RedisItemReader<String, String> reader = reader(context, StringCodec.UTF8).struct();
        String task = String.format(TASK_NAME, resource.getFilename());
        return job(step(reader, writer).task(task));
    }

    private ItemWriter<KeyValue<String>> writer(WritableResource resource) {
        FileDumpType type = dumpFileOptions.type(resource);
        switch (type) {
            case XML:
                XmlResourceItemWriterBuilder<KeyValue<String>> xmlWriterBuilder = new XmlResourceItemWriterBuilder<>();
                xmlWriterBuilder.name("xml-resource-item-writer");
                xmlWriterBuilder.append(fileExportOptions.isAppend());
                xmlWriterBuilder.encoding(fileExportOptions.getEncoding().name());
                xmlWriterBuilder.xmlObjectMarshaller(xmlMarshaller());
                xmlWriterBuilder.lineSeparator(fileExportOptions.getLineSeparator());
                xmlWriterBuilder.rootName(fileExportOptions.getRootName());
                xmlWriterBuilder.resource(resource);
                xmlWriterBuilder.saveState(false);
                return xmlWriterBuilder.build();
            case JSON:
                JsonResourceItemWriterBuilder<KeyValue<String>> jsonWriterBuilder = new JsonResourceItemWriterBuilder<>();
                jsonWriterBuilder.name("json-resource-item-writer");
                jsonWriterBuilder.append(fileExportOptions.isAppend());
                jsonWriterBuilder.encoding(fileExportOptions.getEncoding().name());
                jsonWriterBuilder.jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>());
                jsonWriterBuilder.lineSeparator(fileExportOptions.getLineSeparator());
                jsonWriterBuilder.resource(resource);
                jsonWriterBuilder.saveState(false);
                return jsonWriterBuilder.build();
            default:
                throw new UnsupportedOperationException("Unsupported file type: " + type);
        }
    }

    private JsonObjectMarshaller<KeyValue<String>> xmlMarshaller() {
        XmlMapper mapper = new XmlMapper();
        mapper.setConfig(mapper.getSerializationConfig().withRootName(fileExportOptions.getElementName()));
        JacksonJsonObjectMarshaller<KeyValue<String>> marshaller = new JacksonJsonObjectMarshaller<>();
        marshaller.setObjectMapper(mapper);
        return marshaller;
    }

    @Override
    public String toString() {
        return "FileExport [file=" + file + ", dumpFileOptions=" + dumpFileOptions + ", fileExportOptions=" + fileExportOptions
                + ", readerOptions=" + readerOptions + ", jobOptions=" + jobOptions + "]";
    }

}
