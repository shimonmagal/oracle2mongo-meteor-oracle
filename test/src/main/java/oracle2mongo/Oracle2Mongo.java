package oracle2mongo;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.parser.JSqlParser;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.TablesNamesFinder;
import oracle2mongo.LogEvent.OPERATION;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.io.FileUtils;
import org.bson.BSON;
import org.bson.BsonDocument;
import org.bson.BsonElement;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
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
	private String _mongoUrl;
	private String _mongoDBName;
	private String _configurationString;
	private List<Rule> _rules;
	private int _threadCount;
	private BasicDataSource _bds;
	private long _scn;
	private MongoDatabase _mongoDB;

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
	 * @throws IOException
	 * @throws ParseException
	 */
	public Oracle2Mongo(String jdbcUrl, String mongoUrl, String mongoDBName,
			File configuration, int threadCount) throws IOException,
			ParseException {
		// read file
		this(jdbcUrl, mongoUrl, mongoDBName, FileUtils
				.readFileToString(configuration), threadCount);
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
	public Oracle2Mongo(String jdbcUrl, String mongoUrl, String mongoDBName,
			int threadCount) throws SQLException, ParseException {
		this(jdbcUrl, mongoUrl, mongoDBName,
				createDefaultConfiguratuin(jdbcUrl), threadCount);
	}

	/**
	 * Private constructor - recevies a configuration string
	 * 
	 * @param jdbcUrl
	 * @param mongoUrl
	 * @param mongoDBName
	 * @param configurationString
	 * @throws ParseException
	 */
	private Oracle2Mongo(String jdbcUrl, String mongoUrl, String mongoDBName,
			String configurationString, int threadCount) throws ParseException {
		_jdbcUrl = jdbcUrl;
		_mongoUrl = mongoUrl;
		_mongoDBName = mongoDBName;
		_threadCount = threadCount;
		_configurationString = configurationString.toUpperCase(); // upper case
		ConfigurationParser confParser = new ConfigurationParser(
				_configurationString);
		_rules = confParser.parse();
	}

	public void replicateSnapshot() throws SQLException {
		_bds = new BasicDataSource();
		_bds.setUrl(_jdbcUrl);
		_bds.setDriverClassName("oracle.jdbc.driver.OracleDriver");
		_bds.setMaxActive(_threadCount);
		MongoClient mongoClient = new MongoClient(_mongoUrl);
		_mongoDB = mongoClient.getDatabase(_mongoDBName);
		_scn = getScn(_bds);

		BlockingQueue<Runnable> jobs = new ArrayBlockingQueue<Runnable>(
				_threadCount);
		// execute threads to perform different jobs
		ThreadPoolExecutor tpe = new ThreadPoolExecutor(_threadCount,
				_threadCount, 10000, TimeUnit.SECONDS, jobs);
		for (Rule rule : _rules) {
			tpe.execute(new Oracle2MongoSnapshotWorker(rule, _bds, _mongoDB,
					_scn));
		}

		tpe.shutdown();
	}

	public void replicateContinuent() throws SQLException, JSQLParserException {
		Map<String, List<Rule>> tableToRules = new HashMap<>();
		getTableNamesToRule(_rules, tableToRules);

		long maxScn = 0;
		LinkedList<LogEvent> ll = new LinkedList<LogEvent>();

		try (Connection c = _bds.getConnection();
				PreparedStatement ps = c
						.prepareStatement("select t.*, t.ora_rowscn from log t order by t.ora_rowscn asc");
				ResultSet rs = ps.executeQuery();) {

			while (rs.next()) {
				String table = rs.getString("tablename");
				String fields = rs.getString("fields");
				String ids = rs.getString("ids");
				int op = rs.getInt("op");
				long scn = rs.getLong("ora_rowscn");
				maxScn = maxScn > scn ? maxScn : scn;
				LogEvent le = new LogEvent(table, scn, fields, op, ids);
				ll.add(le);
			}
		}

		if (maxScn > 0) {
			try (Connection c = _bds.getConnection();
					PreparedStatement ps = c
							.prepareStatement("delete from log where ora_rowscn <= ?");) {
				ps.setLong(1, maxScn);
				ps.execute();
			}
		}

		handle(ll, tableToRules);

	}

	private void handle(LinkedList<LogEvent> loggedEvents, Map<String, List<Rule>> tableToRules) throws SQLException {
		for (LogEvent logEvent : loggedEvents) {
			String table = logEvent._tableName.toUpperCase();
			List<Rule> rules = tableToRules.get(table);
			for(Rule rule: rules){
				handleLogEventWithRule(logEvent, rule);
			}
		}
	}

	private void handleLogEventWithRule(LogEvent logEvent, Rule rule) throws SQLException {
		//delete not handled yet
		
		MongoCollection<Document> coll = _mongoDB.getCollection(rule.getCollectionName());
		String sql = null;
		String qMarks = qMarks(logEvent._ids.size());
		if(logEvent._op == OPERATION.INSERT){
			sql = String.format("select * from %s as of scn ? where id in (%s)","%s", logEvent._tableName,qMarks);
		}
		else if(logEvent._op == OPERATION.UPDATE){
			//"," + rule.getLinkSrc()
			sql = String.format("select %s from %s as of scn ? where id in (%s)", logEvent._fields + "," + rule.getIdFieldName() , logEvent._tableName, qMarks);
		}
				
		try(
				Connection connection = _bds.getConnection();
				PreparedStatement ps = connection.prepareStatement(sql);
				){
			int k=0;
			ps.setLong(++k, logEvent._scn);
			for(Long id:logEvent._ids){
				ps.setLong(++k, id);
			}
			
			System.out.println(sql);
			try (ResultSet rs = ps.executeQuery()){
				JSONArray jsonArray = JsonConverter.convert(rs);
				
				if(rule.getParentRule() == null && logEvent._op == OPERATION.INSERT){
					coll.insertMany(JsonConverter.convert(rs));
				}
				else if(rule.getParentRule() == null && logEvent._op == OPERATION.UPDATE){
					
					for(Object jo:jsonArray){
						JSONObject jsonObject = (JSONObject)jo;
						BsonValue val = new BsonString(jsonObject.get(rule.getIdFieldName()).toString());
						Bson x = new BsonDocument("id",val);
						
						List<BsonElement> list = new LinkedList<BsonElement>();
						for(Object key:jsonObject.keySet()){
							list.add(new BsonElement((String)key, new BsonString(jsonObject.get(key).toString())));
						}
						Bson y = new BsonDocument(list );
						coll.updateOne(x, y);
					}
				}
				else if(rule.getParentRule() == null && logEvent._op == OPERATION.DELETE){
					for(Object jo:jsonArray){
						JSONObject jsonObject = (JSONObject)jo;
						BsonValue val = new BsonString(jsonObject.get(rule.getIdFieldName()).toString());
						Bson x = new BsonDocument("id",val);
						coll.deleteOne(x);
					}
				}
				
			}
		}
		
	}

	private String qMarks(int size) {
		StringBuilder sb = new StringBuilder("");
		for(int i=0;i<size;i++){
			sb.append("?");
			if(i<size-1){
				sb.append(",");
			}
		}
		return sb.toString();
	}

	private static Set<String> getQueryTables(String query)
			throws JSQLParserException {
		Set<String> tables = new LinkedHashSet<String>();
		CCJSqlParserManager pm = new CCJSqlParserManager();
		net.sf.jsqlparser.statement.Statement statement = pm
				.parse(new StringReader(query));
		if (statement instanceof Select) {
			Select selectStatement = (Select) statement;
			TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
			List<String> tableList = tablesNamesFinder
					.getTableList(selectStatement);
			for (String table : tableList) {
				tables.add(table);
			}
		}
		return tables;
	}

	private void getTableNamesToRule(List<Rule> rules,
			Map<String, List<Rule>> map) throws JSQLParserException {
		for (Rule rule : rules) {
			String sql = rule.getSql();
			Set<String> tables = getQueryTables(sql);
			for (String table : tables) {
				List<Rule> tabRules = map.get(table);
				if (tabRules == null) {
					tabRules = new LinkedList<Rule>();
					map.put(table, tabRules);
				}
				tabRules.add(rule);
			}

			getTableNamesToRule(rule.getChildRules(), map);
		}
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
