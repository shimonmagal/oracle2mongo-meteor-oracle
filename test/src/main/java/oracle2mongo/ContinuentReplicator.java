package oracle2mongo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import test.test.LogEvent;

public class ContinuentReplicator {

	private long _scn;
	private String _jdbcUrl;
	private String _mongoUrl;
	private String _mongoDBName;

	public ContinuentReplicator(long scn, String jdbcUrl, String mongoUrl, String mongoDBName) {
		_scn = scn;
		_jdbcUrl = jdbcUrl;
		_mongoUrl = mongoUrl;
		_mongoDBName = mongoDBName;
	}
	
	public void replicate() throws SQLException{
		Connection c = DriverManager.getConnection(_jdbcUrl);
		PreparedStatement ps = c.prepareStatement("select t.*, t.ora_rowscn from log t order by t.ora_rowscn asc");
		ResultSet rs = ps.executeQuery();
		LinkedList<LogEvent> ll = new LinkedList<LogEvent>();
		long maxScn = 0;
		while(rs.next()){
			String table = rs.getString("tablename");
			String fields = rs.getString("fields");
			String ids = rs.getString("ids");
			int op = rs.getInt("op");
			long scn = rs.getLong("ora_rowscn");
			maxScn = maxScn > scn ? maxScn : scn; 
			LogEvent le = new LogEvent(table, scn, fields, op, ids);
			ll.add(le);
		}
		
		if(maxScn > 0){
		ps = c.prepareStatement("delete from log where ora_rowscn <= ?");
		ps.setLong(1, maxScn);
		ps.execute();
		ps.close();
		}
		
		handle(ll);
		
		
	}
	
	private void handle(LinkedList<LogEvent> ll) {
		for(LogEvent le:ll){
			List<String> rules = getRules(le._tableName);
			Set<Long> ids = le._ids;
			switch(le._op){
			case INSERT:
				for(rule:rules)
				break;
			case DELETE:
			case UPDATE:
			}
		}
		
	}
	
	private void handleInserts(LinkedList<LogEvent> ll) throws SQLException{
		for(LogEvent le:ll){
			List<String> rules = getRules(le._tableName);
			for(String rule:rules){
				Connection c= getConnection();
				PreparedStatement ps = c.prepareStatement(String.format("select (%s) where id in (%s)"));;
				ResultSet rs = ps.executeQuery();
				String = ConvertToJson(rs);
			}
		}
	}

	private Connection getConnection() {
		return null;
		
	}
	
	private MongoDatabase getMongoDBConnection(){
		MongoClient mongo = new MongoClient(_mongoUrl);
		return mongo.getDatabase(_mongoDBName);
	}

	private List<String> getRules(String _tableName) {
		return null;
	}

	public static void main(String[] args) throws SQLException {
		new ContinuentReplicator(0, "jdbc:oracle:thin:test/test@localhost:1521:xe", "localhost:3001","meteor").replicate();;
	}

}
