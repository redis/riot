package com.redis.riot.cli;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.testcontainers.containers.JdbcDatabaseContainer;

import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.test.AbstractTestBase;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.RedisStackContainer;

import picocli.CommandLine.ExitCode;
import picocli.CommandLine.ParseResult;

abstract class AbstractDbTests extends AbstractRiotTestBase {

    private static final RedisStackContainer REDIS = new RedisStackContainer(
            RedisStackContainer.DEFAULT_IMAGE_NAME.withTag(RedisStackContainer.DEFAULT_TAG));

    protected abstract JdbcDatabaseContainer<?> getJdbcDatabaseContainer();

    protected Connection databaseConnection;

    protected DataSource dataSource;

    @Override
    protected DataType[] generatorDataTypes() {
        return AbstractTestBase.REDIS_MODULES_GENERATOR_TYPES;
    }

    @BeforeAll
    public void setupContainers() throws SQLException {
        JdbcDatabaseContainer<?> container = getJdbcDatabaseContainer();
        container.start();
        DataSourceProperties properties = new DataSourceProperties();
        properties.setUrl(container.getJdbcUrl());
        properties.setUsername(container.getUsername());
        properties.setPassword(container.getPassword());
        dataSource = properties.initializeDataSourceBuilder().build();
        databaseConnection = dataSource.getConnection();
    }

    @AfterAll
    public void teardownContainers() throws SQLException {
        databaseConnection.close();
        getJdbcDatabaseContainer().stop();
    }
    
    @Override
    protected RedisServer getRedisServer() {
        return REDIS;
    }

    @Override
    protected RedisStackContainer getTargetRedisServer() {
        return REDIS;
    }

    protected void executeScript(String file) throws IOException, SQLException {
        SqlScriptRunner scriptRunner = new SqlScriptRunner(databaseConnection);
        scriptRunner.setAutoCommit(false);
        scriptRunner.setStopOnError(true);
        InputStream inputStream = PostgresTests.class.getClassLoader().getResourceAsStream(file);
        if (inputStream == null) {
            throw new FileNotFoundException(file);
        }
        scriptRunner.runScript(new InputStreamReader(inputStream));
    }

    protected int executeDatabaseImport(ParseResult parseResult) {
        DatabaseImportCommand command = command(parseResult);
        configureDatabase(command.args);
        return ExitCode.OK;
    }

    protected int executeDatabaseExport(ParseResult parseResult, TestInfo info) {
        DatabaseExportCommand command = command(parseResult);
        command.setName(name(info));
        configureDatabase(command.args);
        return ExitCode.OK;
    }

    private void configureDatabase(DatabaseArgs args) {
        JdbcDatabaseContainer<?> container = getJdbcDatabaseContainer();
        args.url = container.getJdbcUrl();
        args.username = container.getUsername();
        args.password = container.getPassword();
    }

}
