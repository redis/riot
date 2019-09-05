package com.redislabs.riot;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import javax.sql.DataSource;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;

import com.zaxxer.hikari.HikariDataSource;

public class TestDatabase extends BaseTest {

	@Test
	public void testExportToDatabase() throws Exception {
		populateBeersTable();
		Statement statement = dataSource().getConnection().createStatement();
		statement.execute("SELECT * from beers");
		ResultSet resultSet = statement.getResultSet();
		int count = 0;
		while (resultSet.next()) {
			Assertions.assertTrue(resultSet.getInt("id") > 0);
			count++;
		}
		Assertions.assertEquals(BEER_COUNT, count);
	}

	private void populateBeersTable() throws Exception {
		Connection connection = dataSource().getConnection();
		connection.createStatement().execute("DROP TABLE IF EXISTS beers");
		connection.createStatement()
				.execute("CREATE TABLE IF NOT EXISTS beers (id INT NOT NULL, name VARCHAR(500), PRIMARY KEY (id))");
		runFile("file-import-csv-hash");
		runFile("db-export");
	}

	@Test
	public void testImportDatabase() throws Exception {
		populateBeersTable();
		runFile("db-import");
		List<String> keys = commands().keys("dbbeer:*");
		Assertions.assertEquals(BEER_COUNT, keys.size());
	}

	private DataSource dataSource() {
		DataSourceProperties properties = new DataSourceProperties();
		properties.setUrl("jdbc:hsqldb:mem:mymemdb");
		properties.setDriverClassName("org.hsqldb.jdbc.JDBCDriver");
		return properties.initializeDataSourceBuilder().type(HikariDataSource.class).build();
	}

}
