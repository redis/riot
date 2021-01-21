package com.redislabs.riot.file;

import com.redislabs.riot.AbstractTransferCommand;
import com.redislabs.riot.StepBuilder;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.redis.DataStructureItemWriter;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.batch.item.xml.XmlItemReader;
import org.springframework.core.io.Resource;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Setter
@Command(name = "import-dump", description = "Import data-structure dump file(s) into Redis")
public class DataStructureFileImportCommand extends AbstractTransferCommand<DataStructure<String>, DataStructure<String>> {

    @CommandLine.Parameters(arity = "1..*", description = "One ore more files or URLs", paramLabel = "FILE")
    private String[] files = new String[0];
    @Getter
    @CommandLine.Mixin
    private FileOptions fileOptions = new FileOptions();

    @Override
    protected Flow flow() throws Exception {
        String[] expandedFiles = FileUtils.expand(files);
        List<Step> steps = new ArrayList<>();
        DataStructureItemProcessor processor = new DataStructureItemProcessor();
        for (String file : expandedFiles) {
            FileType fileType = FileUtils.fileType(file);
            Resource resource = FileUtils.inputResource(file, fileOptions);
            String name = FileUtils.filename(resource);
            StepBuilder<DataStructure<String>, DataStructure<String>> step = stepBuilder("Importing file " + name);
            AbstractItemStreamItemReader<DataStructure<String>> reader = reader(file, fileType, resource);
            reader.setName(name);
            steps.add(step.reader(reader).processor(processor).writer(writer()).build().build());
        }
        return flow(steps.toArray(new Step[0]));
    }

    private ItemWriter<DataStructure<String>> writer() {
        if (isCluster()) {
            return DataStructureItemWriter.clusterBuilder((GenericObjectPool<StatefulRedisClusterConnection<String, String>>) pool).commandTimeout(getCommandTimeout()).build();
        }
        return DataStructureItemWriter.builder((GenericObjectPool<StatefulRedisConnection<String, String>>) pool).commandTimeout(getCommandTimeout()).build();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    protected AbstractItemStreamItemReader<DataStructure<String>> reader(String file, FileType fileType, Resource resource) {
        switch (fileType) {
            case JSON:
                return (JsonItemReader) FileUtils.jsonReader(resource, DataStructure.class);
            case XML:
                return (XmlItemReader) FileUtils.xmlReader(resource, DataStructure.class);
        }
        throw new IllegalArgumentException("Unsupported file type: " + fileType);
    }

}
