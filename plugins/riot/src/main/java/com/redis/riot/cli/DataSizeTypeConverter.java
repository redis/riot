package com.redis.riot.cli;

import org.springframework.util.unit.DataSize;
import org.springframework.util.unit.DataUnit;

import picocli.CommandLine.ITypeConverter;

public class DataSizeTypeConverter implements ITypeConverter<DataSize> {

    @Override
    public DataSize convert(String value) {
        return DataSize.parse(value, DataUnit.MEGABYTES);
    }

}
