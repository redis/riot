package com.redislabs.riot.file;

import com.redislabs.riot.test.BaseTest;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings({"rawtypes", "unchecked"})
public class AbstractFileTest extends BaseTest {

    protected final static int COUNT = 2410;

    @Override
    protected int execute(String[] args) {
        return new RiotFile().execute(args);
    }

    @Override
    protected String applicationName() {
        return "riot-file";
    }


    protected <T> List<T> readAll(AbstractItemCountingItemStreamItemReader<T> reader) throws Exception {
        reader.open(new ExecutionContext());
        List<T> records = new ArrayList<>();
        T record;
        while ((record = reader.read()) != null) {
            records.add(record);
        }
        reader.close();
        return records;
    }



}
