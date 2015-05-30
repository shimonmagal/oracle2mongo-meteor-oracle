package oracle2mongo;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

public class Oracle2Mongo {

	static {
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	private String _jdbcUrl;
	private String _mongoUrl;
	private String _mongoDBName;
	private String _configurationString;
	private List<Rule> _rules;

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
	 * @throws ParseException 
	 */
	public Oracle2Mongo(String jdbcUrl, String mongoUrl, String mongoDBName,
			File configuration) throws IOException, ParseException {
		// read file
		this(jdbcUrl, mongoUrl, mongoDBName, FileUtils
				.readFileToString(configuration));
	}

	/**
	 * @see Oracle2Mongo#Oracle2Mongo(String, String, String, File)
	 * They are basically the same, but here there is no need to pass configuration,
	 * if you are interested in default configuration - each table in oracle become a
	 * collection in mongo, Every record becomes a document.
	 * @param jdbcUrl
	 * @param mongoUrl
	 * @param mongoDBName
	 * @throws SQLException
	 * @throws ParseException 
	 */
	public Oracle2Mongo(String jdbcUrl, String mongoUrl, String mongoDBName)
			throws SQLException, ParseException {
		this(jdbcUrl, mongoUrl, mongoDBName,
				createDefaultConfiguratuin(jdbcUrl));
	}

	/**
	 * Private constructor - recevies a configuration string
	 * @param jdbcUrl
	 * @param mongoUrl
	 * @param mongoDBName
	 * @param configurationString
	 * @throws ParseException 
	 */
	private Oracle2Mongo(String jdbcUrl, String mongoUrl, String mongoDBName,
			String configurationString) throws ParseException {
		_jdbcUrl = jdbcUrl;
		_mongoUrl = mongoUrl;
		_mongoDBName = mongoDBName;
		_configurationString = configurationString.toUpperCase(); //upper case
		ConfigurationParser confParser = new ConfigurationParser(_configurationString);
		_rules = confParser.parse();
	}
	
	public void replicateSnapshot(){
		
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
						.prepareStatement("select table_name from user_tables");
				ResultSet rs = ps.executeQuery();) {
			JSONArray ja = new JSONArray();
			while (rs.next()) {
				String tablename = rs.getString("table_name");
				JSONObject jo = new JSONObject();
				JSONObject queryDetails = new JSONObject();
				jo.put("collection", tablename);
				queryDetails.put("sql", String.format("select * from %s", tablename));
				jo.put("rule",queryDetails);
				ja.add(jo);
			}
			
			return ja.toJSONString();
		}
	}

}
