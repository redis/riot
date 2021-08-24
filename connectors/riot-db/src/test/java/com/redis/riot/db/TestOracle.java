package com.redis.riot.db;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;

public class TestOracle {

	public static void main(String[] args) throws IOException, SQLException {
		DataSourceProperties properties = new DataSourceProperties();
		properties.setUrl("jdbc:oracle:thin:@localhost:51521:XE");
		properties.setUsername("sys as sysdba");
		properties.setPassword("mysecurepassword");
		DataSource dataSource = properties.initializeDataSourceBuilder().build();
		Connection connection = dataSource.getConnection();
		ScriptRunner scriptRunner = ScriptRunner.builder().connection(connection).autoCommit(false).stopOnError(true)
				.build();
		InputStream inputStream = TestOracle.class.getClassLoader().getResourceAsStream("oracle.sql");
		scriptRunner.runScript(new InputStreamReader(inputStream));
		Statement statement = connection.createStatement();
		statement.execute("SELECT COUNT(*) AS count FROM employees");
		ResultSet resultSet = statement.getResultSet();
		resultSet.next();
		System.out.println(resultSet.getLong("count"));
		connection.close();
	}

}
