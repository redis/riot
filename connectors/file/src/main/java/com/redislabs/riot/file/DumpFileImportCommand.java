package com.redislabs.riot.file;

import com.redislabs.riot.AbstractTransferCommand;
import com.redislabs.riot.StepBuilder;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.redis.RedisClusterDataStructureItemWriter;
import org.springframework.batch.item.redis.RedisDataStructureItemWriter;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.batch.item.xml.XmlItemReader;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Setter
@Command(name = "import-dump", description = "Import Redis data files into Redis")
public class DumpFileImportCommand extends AbstractTransferCommand<DataStructure<String>, DataStructure<String>> {

    @CommandLine.Parameters(arity = "0..*", description = "One ore more files or URLs", paramLabel = "FILE")
    private String[] files;
    @CommandLine.Option(names = {"-t", "--type"}, description = "File type: ${COMPLETION-CANDIDATES}", paramLabel = "<type>")
    private DumpFileType type;
    @CommandLine.Mixin
    private FileOptions fileOptions = FileOptions.builder().build();

    @Override
    protected Flow flow() throws Exception {
        Assert.isTrue(!ObjectUtils.isEmpty(files), "No file specified");
        List<String> expandedFiles = FileUtils.expand(files);
        if (ObjectUtils.isEmpty(expandedFiles)) {
            throw new FileNotFoundException("File not found: " + String.join(", ", files));
        }
        List<Step> steps = new ArrayList<>();
        DataStructureItemProcessor processor = new DataStructureItemProcessor();
        for (String file : expandedFiles) {
            DumpFileType fileType = type == null ? DumpFileType.of(file) : type;
            Resource resource = FileUtils.inputResource(file, fileOptions);
            String name = FileUtils.filename(resource);
            AbstractItemStreamItemReader<DataStructure<String>> reader = reader(fileType, resource);
            reader.setName(name);
            StepBuilder<DataStructure<String>, DataStructure<String>> step = stepBuilder(name + "-datastructure-file-import-step", "Importing " + name);
            steps.add(step.reader(reader).processor(processor).writer(writer()).build().build());
        }
        return flow(steps.toArray(new Step[0]));
    }

    @SuppressWarnings("unchecked")
    private ItemWriter<DataStructure<String>> writer() {
        if (isCluster()) {
            return new RedisClusterDataStructureItemWriter<>((GenericObjectPool<StatefulRedisClusterConnection<String, String>>) pool);
        }
        return new RedisDataStructureItemWriter<>((GenericObjectPool<StatefulRedisConnection<String, String>>) pool);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    protected AbstractItemStreamItemReader<DataStructure<String>> reader(DumpFileType fileType, Resource resource) {
        if (fileType == DumpFileType.XML) {
            log.info("Creating XML data structure reader for file {}", resource);
            return (XmlItemReader) FileUtils.xmlReader(resource, DataStructure.class);
        }
        log.info("Creating JSON data structure reader for file {}", resource);
        return (JsonItemReader) FileUtils.jsonReader(resource, DataStructure.class);
    }

}
