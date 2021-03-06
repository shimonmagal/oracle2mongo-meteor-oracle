package oracle2;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import net.sf.jsqlparser.JSQLParserException;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.io.FileUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

public class Oracle2Mongo {

	private static final String SELECT_ALL_TABLES_WITH_PRIMARY_KEYS = "SELECT cols.table_name, cols.column_name "
			+ "FROM user_constraints cons, user_cons_columns cols "
			+ "where cons.constraint_type = 'P' AND cons.constraint_name = cols.constraint_name "
			+ "AND cons.owner = cols.owner ORDER BY cols.table_name, cols.position";

	static {
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	private String _jdbcUrl;
	private String _configurationString;
	private List<Rule> _rules;
	private int _threadCount;
	private BasicDataSource _bds;
	private long _scn;
	private DBWrapper _destDB;
	private ContinuentReplicator _continentReplicator;

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
	 * @param threadCount
	 *            - number of threads to use
	 * @param destDB 
	 * @throws IOException
	 * @throws ParseException
	 * @throws SQLException 
	 */
	public Oracle2Mongo(String jdbcUrl, File configuration, int threadCount, DBWrapper destDB) throws IOException,
			ParseException, SQLException {
		// read file
		this(jdbcUrl, FileUtils
				.readFileToString(configuration), threadCount, destDB);
	}

	/**
	 * @see Oracle2Mongo#Oracle2Mongo(String, String, String, File) They are
	 *      basically the same, but here there is no need to pass configuration,
	 *      if you are interested in default configuration - each table in
	 *      oracle become a collection in mongo, Every record becomes a
	 *      document.
	 * @param jdbcUrl
	 * @param mongoUrl
	 * @param mongoDBName
	 * @param threadCount
	 *            - number of threads to use
	 * @throws SQLException
	 * @throws ParseException
	 */
	public Oracle2Mongo(String jdbcUrl, DBWrapper destDB,
			int threadCount) throws SQLException, ParseException {
		this(jdbcUrl,
				createDefaultConfiguratuin(jdbcUrl), threadCount, destDB);
	}

	/**
	 * Private constructor - recevies a configuration string
	 * 
	 * @param jdbcUrl
	 * @param mongoUrl
	 * @param mongoDBName
	 * @param configurationString
	 * @param destDB 
	 * @throws ParseException
	 * @throws SQLException 
	 */
	private Oracle2Mongo(String jdbcUrl, 
			String configurationString, int threadCount, DBWrapper destDB) throws ParseException, SQLException {
		_jdbcUrl = jdbcUrl;
		_threadCount = threadCount;
		_configurationString = configurationString.toUpperCase(); // upper case
		ConfigurationParser confParser = new ConfigurationParser(
				_configurationString);
		_rules = confParser.parse();
		_bds = new BasicDataSource();
		_bds.setUrl(_jdbcUrl);
		_bds.setDriverClassName("oracle.jdbc.driver.OracleDriver");
		_bds.setMaxActive(_threadCount);
		_destDB = destDB;
		_scn = getScn(_bds);
		
		_continentReplicator = new ContinuentReplicator(_bds, _destDB, _scn, _rules);

	}

	public void replicateSnapshot() throws SQLException {

		BlockingQueue<Runnable> jobs = new ArrayBlockingQueue<Runnable>(
				_threadCount);
		// execute threads to perform different jobs
		ThreadPoolExecutor tpe = new ThreadPoolExecutor(_threadCount,
				_threadCount, 10000, TimeUnit.SECONDS, jobs);
		for (Rule rule : _rules) {
			tpe.execute(new Oracle2SnapshotWorker(rule, _bds, _destDB,
					_scn));
		}

		tpe.shutdown();
	}
	
	public void replicateContinuent() throws SQLException, JSQLParserException{
		_continentReplicator.replicateContinuent();
	}


	private long getScn(BasicDataSource bds) throws SQLException {
		try (Connection c = bds.getConnection();
				PreparedStatement ps = c
						.prepareStatement("select current_scn from gv$database");
				ResultSet rs = ps.executeQuery();) {
			rs.next();
			return rs.getLong("CURRENT_SCN");

		}
	}

	/**
	 * Creates a default configuration - that is, every table == collections,
	 * and each field in table == field in collection. This is done by
	 * generating a basic default configuration, based on all the tables in your
	 * scheme that have a primary key. Others are ignored. The primary key
	 * coulmn shall become the document's "_id".
	 * 
	 * @param jdbcUrl
	 * @return string that represents the configuration
	 * @throws SQLException
	 */
	private static String createDefaultConfiguratuin(String jdbcUrl)
			throws SQLException {
		try (Connection connection = DriverManager.getConnection(jdbcUrl);
				PreparedStatement ps = connection
						.prepareStatement(SELECT_ALL_TABLES_WITH_PRIMARY_KEYS);
				ResultSet rs = ps.executeQuery();) {
			JSONArray ja = new JSONArray();
			while (rs.next()) {
				String tablename = rs.getString("table_name");
				String columnName = rs.getString("column_name");
				JSONObject jo = new JSONObject();
				JSONObject queryDetails = new JSONObject();
				jo.put("COLLECTION", tablename);
				queryDetails.put("sql",
						String.format("select * from %s", tablename));
				queryDetails.put("ID_FIELD", columnName);
				jo.put("RULE", queryDetails);
				ja.add(jo);
			}

			return ja.toJSONString();
		}
	}

}
