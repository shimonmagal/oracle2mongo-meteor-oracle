package oracle2;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.TablesNamesFinder;
import oracle2.LogEvent.OPERATION;

import org.bson.BsonDocument;
import org.bson.BsonElement;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;

public class ContinuentReplicator {

	private MongoDatabase _mongoDB;
	private Object _scn;
	private DataSource _ds;
	private List<Rule> _rules;

	public ContinuentReplicator(DataSource ds, MongoDatabase mongoDB, long scn,
			List<Rule> rules) {
		_ds = ds;
		_mongoDB = mongoDB;
		_scn = scn;
		_rules = rules;
	}

	public void replicateContinuent() throws SQLException, JSQLParserException {
		Map<String, List<Rule>> tableToRules = new HashMap<>();
		getTableNamesToRule(_rules, tableToRules);

		long maxScn = 0;
		LinkedList<LogEvent> ll = new LinkedList<LogEvent>();

		try (Connection c = _ds.getConnection();
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
			try (Connection c = _ds.getConnection();
					PreparedStatement ps = c
							.prepareStatement("delete from log where ora_rowscn <= ?");) {
				ps.setLong(1, maxScn);
				ps.execute();
			}
		}

		handle(ll, tableToRules);

	}

	private void handle(LinkedList<LogEvent> loggedEvents,
			Map<String, List<Rule>> tableToRules) throws SQLException {
		for (LogEvent logEvent : loggedEvents) {
			String table = logEvent._tableName.toUpperCase();
			List<Rule> rules = tableToRules.get(table);
			for (Rule rule : rules) {
				handleLogEventWithRule(logEvent, rule);
			}
		}
	}

	private void handleLogEventWithRule(LogEvent logEvent, Rule rule)
			throws SQLException {
		MongoCollection<Document> coll = _mongoDB.getCollection(cascadeRule3(rule)
				.getCollectionName());
		String sql = null;
		String qMarks = qMarks(logEvent._ids.size());
		if (logEvent._op == OPERATION.INSERT) {
			sql = String.format(
					"select * from %s as of scn ? where id in (%s)",
					logEvent._tableName, qMarks);
		} else if (logEvent._op == OPERATION.UPDATE) {
			// "," + rule.getLinkSrc()
			sql = String.format(
					"select %s from %s as of scn ? where id in (%s)",
					logEvent._fields + "," + rule.getIdFieldName(),
					logEvent._tableName, qMarks);
		}

		if (rule.getParentRule() == null && logEvent._op == OPERATION.DELETE) {
			for (Long id : logEvent._ids) {
				BsonValue val = new BsonString(id+"");
				Bson x = new BsonDocument("ID", val);
				System.out.println(rule.getIdFieldName() + " " + val);
				System.out.println(x);
				coll.deleteMany(x);

				System.out.println("delete");
			}
			return;
		}

		try (Connection connection = _ds.getConnection();
				PreparedStatement ps = connection.prepareStatement(sql);) {
			if(sql != null){
			System.out.println(sql);
			int k = 0;
			ps.setLong(++k, logEvent._scn);
			for (Long id : logEvent._ids) {
				ps.setLong(++k, id);
			}
			}

			System.out.println(sql);
			try (ResultSet rs = ps.executeQuery()) {
				JSONArray jsonArray = JsonConverter.convert(rs);

				if (rule.getParentRule() == null
						&& logEvent._op == OPERATION.INSERT) {
					List<Document> docs = new LinkedList<>();
					for (Object elem : jsonArray) {
						JSONObject joe = (JSONObject) elem;
						Map<String, Object> map = new HashMap<>();
						for (Object key : joe.keySet()) {
							map.put(key.toString(), joe.get(key).toString());
						}
						Document doc = new Document(map);
						docs.add(doc);
					}
					coll.insertMany(docs);
				} else if (rule.getParentRule() == null
						&& logEvent._op == OPERATION.UPDATE) {
					for (Object elem : jsonArray) {
						BsonValue val = new BsonString(
								""+(Long.parseLong(((JSONObject) elem).get(
										rule.getIdFieldName()).toString())));
						Bson x = new BsonDocument("ID", val);
						JSONObject joe = (JSONObject) elem;

						List<BsonElement> bsonElements = new LinkedList<>();

						for (Object key : joe.keySet()) {
							String keyS = key.toString();
							Document doc = new Document("$set", new Document(
									keyS, joe.get(keyS).toString()));
							System.out.println(x);
							System.out.println(doc);
							coll.updateOne(x, doc);
						}
					}
				}

				else if (logEvent._op == OPERATION.INSERT) {
					// Document doc = new Document("$push", new Document(keyS,
					// joe.get(keyS).toString()));
					for (Object elem : jsonArray) {
						JSONObject jo = (JSONObject)elem;
							String doc = "{\"" + cascadeRule(rule.getParentRule()) + "ID\":\""
									+ jo.get(rule.getLinkSrc().toUpperCase()) + "\"}";
							System.out.println(doc);
							BsonDocument filterDocument = BsonDocument
									.parse(doc);
							String doc2 = cascade2(rule);
							Document doc3 = docify(elem);
							String doc4 = "{$push : {\""+ doc2.toString() +"\":" + doc3.toJson() + "}}";
							System.out.println("doc4" + doc4);
							BsonDocument infoDocument = BsonDocument
									.parse(doc4);
							System.out.println("infoDoc:" + infoDocument);
							UpdateResult r = coll.updateOne(filterDocument, infoDocument);
							System.out.println(r.getModifiedCount());
					}
				}
				
				
				
				
				
				else if (logEvent._op == OPERATION.DELETE) {
					// Document doc = new Document("$push", new Document(keyS,
					// joe.get(keyS).toString()));
					for (Long id:logEvent._ids) {
						
						/*
						 * 
						 * meteor:PRIMARY> db.x.update({"a.b.c.d._id": 111}, {$pull: {"a.0.b.0.c.0.d" : {_id:111}}});
						 */
							String doc = "{\"" + cascadeRule4(rule.getParentRule()) + "ID\":\""
									+ id + "\"}";
							System.out.println(doc);
							BsonDocument filterDocument = BsonDocument
									.parse(doc);
							String doc4 = "{$pull : {\""+ cascade5(rule) +"\":\"" + "{\"ID\": }" + "\"}}";
							System.out.println("doc4" + doc4);
							BsonDocument infoDocument = BsonDocument
									.parse(doc4);
							System.out.println("infoDoc:" + infoDocument);
							UpdateResult r = coll.updateOne(filterDocument, infoDocument);
							System.out.println(r.getModifiedCount());
					}
				}

			}
		}

	}

	private Rule cascadeRule3(Rule rule) {
		if(rule.getParentRule() == null)
			return rule;
		return cascadeRule3(rule.getParentRule());
	}

	private Document docify(Object elem) {
		JSONObject joe = (JSONObject) elem;
		Map<String, Object> map = new HashMap<>();
		for (Object key : joe.keySet()) {
			map.put(key.toString(), joe.get(key).toString());
		}
		Document doc = new Document(map);
		return doc;

	}

	private String cascade2(Rule rule) {
		if (rule == null)
			return "";
		if (rule.getParentRule() == null)
			return"";
		if (rule.getParentRule().getParentRule() == null)
			return rule.getCollectionName();
		return cascade2(rule.getParentRule()) +".$."+ rule.getCollectionName() + ".0";

		// a: [{b: [{c: [{"_id":2}]}]}]
	}
	
	private String cascade5(Rule rule) {
		if (rule == null)
			return "";
		if (rule.getParentRule() == null)
			return rule.getCollectionName();
		if (rule.getParentRule().getParentRule() == null)
			return rule.getCollectionName();
		return cascade2(rule.getParentRule()) +".$."+ rule.getCollectionName() + ".0";

		// a: [{b: [{c: [{"_id":2}]}]}]
	}


	private String cascadeRule(Rule rule) {
		if (rule.getParentRule() == null)
			return "";
		return cascadeRule(rule.getParentRule()) + "."
				+ rule.getCollectionName() + ".";
	}
	
	private String cascadeRule4(Rule rule) {
		if (rule.getParentRule() == null)
			return "";
		if (rule.getParentRule().getParentRule() == null)
			return rule.getCollectionName();
		return cascadeRule(rule.getParentRule()) + "."
				+ rule.getCollectionName() + ".";
	}


	private String qMarks(int size) {
		StringBuilder sb = new StringBuilder("");
		for (int i = 0; i < size; i++) {
			sb.append("?");
			if (i < size - 1) {
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

}
