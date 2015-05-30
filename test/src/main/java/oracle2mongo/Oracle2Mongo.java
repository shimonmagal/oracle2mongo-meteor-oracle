package oracle2mongo;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class Oracle2Mongo {

	static {
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * creates a replicator between oracle and mongoDB
	 * 
	 * @param jdbcUrl
	 *            - jdbc url to oracle db, e.g.:
	 *            jdbc:oracle:thin:user/pass@localhost:1521:xe
	 * @param mongoUrl
	 *            - url to mongo db, e.g.: localhost:3001
	 * @param mongoDBName
	 *            - name of the mongo db, e.g.: meteor
	 * @param configuration
	 *            - a configuration file, please refer to configuration
	 *            documentation
	 * @throws IOException
	 */
	public Oracle2Mongo(String jdbcUrl, String mongoUrl, String mongoDBName,
			File configuration) throws IOException {
		// read file
		this(jdbcUrl, mongoUrl, mongoDBName, FileUtils
				.readFileToString(configuration));
	}

	public Oracle2Mongo(String jdbcUrl, String mongoUrl, String mongoDBName)
			throws SQLException {
		this(jdbcUrl, mongoUrl, mongoDBName,
				createDefaultConfiguratuin(jdbcUrl));
	}

	private Oracle2Mongo(String jdbcUrl, String mongoUrl, String mongoDBName,
			String configurationString) {

	}

	/**
	 * Creates a default configuration - that is, every table == collections,
	 * and each field in table == field in collection.
	 * This is done by generating a basic default configuration, based on all the tables
	 * in your scheme.
	 * 
	 * @param jdbcUrl
	 * @return string that represents the configuration
	 * @throws SQLException
	 */
	private static String createDefaultConfiguratuin(String jdbcUrl)
			throws SQLException {
		try (Connection connection = DriverManager.getConnection(jdbcUrl);
				PreparedStatement ps = connection
						.prepareStatement("select tablename from user_tables");
				ResultSet rs = ps.executeQuery();) {
			JSONArray ja = new JSONArray();
			while (rs.next()) {
				String tablename = rs.getString("tablename");
				JSONObject jo = new JSONObject();
				JSONObject queryDetails = new JSONObject();
				jo.put("collection", tablename);
				queryDetails.put("sql", String.format("select * from %s", tablename));
				jo.put("query",queryDetails);
				ja.add(jo);
			}
			
			return ja.toJSONString();
		}
	}

}
