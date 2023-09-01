package com.redis.riot.core.file;

import java.io.IOException;

import org.springframework.batch.core.Job;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.JsonObjectMarshaller;
import org.springframework.batch.item.support.AbstractFileItemWriter;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redis.riot.core.AbstractExport;
import com.redis.riot.core.RiotExecutionException;
import com.redis.riot.core.file.resource.JsonResourceItemWriter;
import com.redis.riot.core.file.resource.JsonResourceItemWriterBuilder;
import com.redis.riot.core.file.resource.XmlResourceItemWriter;
import com.redis.riot.core.file.resource.XmlResourceItemWriterBuilder;
import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.ValueType;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.StringCodec;

public class FileDumpExport extends AbstractExport {

    public static final String DEFAULT_ELEMENT_NAME = "record";

    public static final String DEFAULT_ROOT_NAME = "root";

    public static final String DEFAULT_LINE_SEPARATOR = AbstractFileItemWriter.DEFAULT_LINE_SEPARATOR;

    private final String file;

    private FileOptions fileOptions = new FileOptions();

    private boolean append;

    private String rootName = DEFAULT_ROOT_NAME;

    private String elementName = DEFAULT_ELEMENT_NAME;

    private String lineSeparator = DEFAULT_LINE_SEPARATOR;

    private FileDumpType type;

    public FileOptions getFileOptions() {
        return fileOptions;
    }

    public void setFileOptions(FileOptions fileOptions) {
        this.fileOptions = fileOptions;
    }

    public FileDumpType getType() {
        return type;
    }

    public void setType(FileDumpType type) {
        this.type = type;
    }

    public boolean isAppend() {
        return append;
    }

    public void setAppend(boolean append) {
        this.append = append;
    }

    public String getRootName() {
        return rootName;
    }

    public void setRootName(String rootName) {
        this.rootName = rootName;
    }

    public String getElementName() {
        return elementName;
    }

    public void setElementName(String elementName) {
        this.elementName = elementName;
    }

    public String getLineSeparator() {
        return lineSeparator;
    }

    public void setLineSeparator(String lineSeparator) {
        this.lineSeparator = lineSeparator;
    }

    public FileDumpExport(AbstractRedisClient client, String file) {
        super(client);
        this.file = file;
    }

    @Override
    protected Job job() {
        return jobBuilder().start(step(getName()).reader(reader(StringCodec.UTF8)).writer(writer()).build().build()).build();
    }

    private ItemWriter<KeyValue<String>> writer() {
        WritableResource resource;
        try {
            resource = FileUtils.outputResource(file, fileOptions);
        } catch (IOException e) {
            throw new RiotExecutionException("Could not open file for writing: " + file, e);
        }
        if (dumpType(resource) == FileDumpType.XML) {
            return xmlWriter(resource);
        }
        return jsonWriter(resource);
    }

    private FileDumpType dumpType(WritableResource resource) {
        if (type == null) {
            return FileUtils.dumpType(resource);
        }
        return type;
    }

    private JsonResourceItemWriter<KeyValue<String>> jsonWriter(Resource resource) {
        JsonResourceItemWriterBuilder<KeyValue<String>> jsonWriterBuilder = new JsonResourceItemWriterBuilder<>();
        jsonWriterBuilder.name("json-resource-item-writer");
        jsonWriterBuilder.append(append);
        jsonWriterBuilder.encoding(fileOptions.getEncoding());
        jsonWriterBuilder.jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>());
        jsonWriterBuilder.lineSeparator(lineSeparator);
        jsonWriterBuilder.resource(resource);
        jsonWriterBuilder.saveState(false);
        return jsonWriterBuilder.build();
    }

    private XmlResourceItemWriter<KeyValue<String>> xmlWriter(Resource resource) {
        XmlResourceItemWriterBuilder<KeyValue<String>> xmlWriterBuilder = new XmlResourceItemWriterBuilder<>();
        xmlWriterBuilder.name("xml-resource-item-writer");
        xmlWriterBuilder.append(append);
        xmlWriterBuilder.encoding(fileOptions.getEncoding());
        xmlWriterBuilder.xmlObjectMarshaller(xmlMarshaller());
        xmlWriterBuilder.lineSeparator(lineSeparator);
        xmlWriterBuilder.rootName(rootName);
        xmlWriterBuilder.resource(resource);
        xmlWriterBuilder.saveState(false);
        return xmlWriterBuilder.build();
    }

    private JsonObjectMarshaller<KeyValue<String>> xmlMarshaller() {
        XmlMapper mapper = new XmlMapper();
        mapper.setConfig(mapper.getSerializationConfig().withRootName(elementName));
        JacksonJsonObjectMarshaller<KeyValue<String>> marshaller = new JacksonJsonObjectMarshaller<>();
        marshaller.setObjectMapper(mapper);
        return marshaller;
    }

    @Override
    protected ValueType getValueType() {
        return ValueType.STRUCT;
    }

}
