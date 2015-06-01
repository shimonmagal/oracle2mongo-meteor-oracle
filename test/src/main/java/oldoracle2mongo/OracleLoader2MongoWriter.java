package oldoracle2mongo;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.TablesNamesFinder;
import oracle2mongo.JsonConverter;

import org.bson.Document;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class OracleLoader2MongoWriter implements Runnable, Comparable<OracleLoader2MongoWriter>{
	
	static{
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	private String _jdbcUrl;
	private String _mongoUrl;
	private String _mongoDBName;
	private MongoDatabase _mongoDB;
	private JSONObject _confRule;
	private ConcurrentMap<String, Collection<String>> _tableMap;

	public OracleLoader2MongoWriter(JSONObject confRule, String jdbcUrl, String mongoUrl, String mongoDBName, ConcurrentMap<String, Collection<String>> tableMap) throws SQLException {
		_confRule = confRule;
		_jdbcUrl = jdbcUrl;
		_mongoUrl = mongoUrl;
		MongoClient mongo = new MongoClient("localhost",3001);
		_mongoDBName = mongoDBName;
		System.out.println("~~~" + _mongoDBName);
		_mongoDB = mongo.getDatabase(_mongoDBName);
		_tableMap = tableMap;

	}

	public void run() {
		try(Connection con = DriverManager.getConnection(_jdbcUrl);){
			JSONArray res = work(con, _confRule);
			
			for(Object elem: res){
				JSONObject jo = (JSONObject) elem;
				String coll = getCollectionName(jo);
				System.out.println("---->" + coll);
				MongoCollection<Document> collection = _mongoDB.getCollection(coll);
				Document doc = Document.parse(((JSONArray)jo.get(coll)).get(0).toString());
				System.out.println(doc);
				collection.insertOne(doc );
				FindIterable<Document> w = collection.find();
				for(Document ww:w){
					System.out.println(ww);
				}
			}
		} catch (SQLException | JSQLParserException e) {
			e.printStackTrace();
		}
	}

	private String getCollectionName(JSONObject jo) {
		String text = (String) jo.keySet().iterator().next();
		return text;
	}

	private JSONArray work(Connection con, JSONObject rule) throws SQLException, JSQLParserException {
		JSONObject query = (JSONObject) rule.get("QUERY");
		String collectionName = (String) rule.get("COLLECTION");
		String sql = (String) query.get("SQL");
		handleSql(sql);//handle sql - refactor later
		String linkSrc = (String) query.get("LINKSRC");
		String linkDest = (String) query.get("LINKDEST");
		JSONArray subqueries = (JSONArray) query.get("SUBQUERIES");
		List<JSONArray> subcollections = new LinkedList<JSONArray>();
		if(subqueries != null){
			for(Object subquery:subqueries){
				JSONArray workDone = work(con, (JSONObject) subquery);
				subcollections.add(workDone);
			}
		}
		String sql2 = sql + (linkSrc==null?"":(" order by " + linkSrc + " asc"));
		try(
			PreparedStatement ps = con.prepareStatement(sql2);
			ResultSet rs = ps.executeQuery();
		){
			JSONArray json = JsonConverter.convert(rs);
			
			System.out.println("~~~" + json);
			
			for(JSONArray subcol:subcollections){
				System.out.println("w" + subcol);
				join(json, subcol);	
			}
			
			JSONArray jsonArr = rearrange(json, linkSrc, collectionName);
			
			System.out.println("========================");
			System.out.println(jsonArr);
			System.out.println("========================");
			return jsonArr;
		}
		
	}


	private void handleSql(String sql) throws JSQLParserException {
		if(sql == null){
			return;
		}
		
		Set<String> tables = getQueryTables(sql);
		for(String table:tables){
			Collection<String>list = new ConcurrentLinkedQueue<String>();
			list = _tableMap.putIfAbsent(table, list);
		}
		
	}
	
	private static Set<String> getQueryTables(String query) throws JSQLParserException{
		Set<String> tables = new LinkedHashSet<String>();
		CCJSqlParserManager pm = new CCJSqlParserManager();
		net.sf.jsqlparser.statement.Statement statement = pm.parse(new StringReader(query));
		if (statement instanceof Select) {
			Select selectStatement = (Select) statement;
			TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
			List<String> tableList = tablesNamesFinder.getTableList(selectStatement);
			for(String table:tableList){
				tables.add(table);
			}
		}
		return tables;
	}

	private void join(JSONArray jsonArr, JSONArray subcol) {
		//should be same size
		for(int i=0;i<jsonArr.size();i++){
			JSONObject jo = (JSONObject) subcol.get(i);
			String text = getCollectionName(jo);
			((JSONObject)jsonArr.get(i)).put(text, ((JSONObject)subcol.get(i)).get(text));
		}
	}

	private JSONArray rearrange(JSONArray json, String linkSrc,
			String collectionName) {
		JSONArray ja = new JSONArray();
		JSONObject currObject = null;
		
		Object linkSrcField = null;
		for(Object elem:json){
			JSONObject jo = (JSONObject)elem;
			Object newLinkSrcField = jo.get(linkSrc);
			
			

			if(linkSrcField == null || !newLinkSrcField.equals(linkSrcField)){
				currObject = new JSONObject();
				currObject.put(collectionName, new JSONArray());
				ja.add(currObject);
			}
			linkSrcField = jo.get(linkSrc);
			JSONArray tempJsonArray = (JSONArray) currObject.get(collectionName);
			tempJsonArray.add(elem);
		}
		
		System.out.println("rearranged:" + ja);
		return ja;
	}

	public int compareTo(OracleLoader2MongoWriter o) {
		return 0;
	}

}