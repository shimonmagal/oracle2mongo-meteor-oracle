package oracle2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import javax.sql.DataSource;

import org.bson.Document;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class Oracle2SnapshotWorker implements Runnable{

	private DBWrapper _destDBWrapper;
	private DataSource _dbDataSource;
	private Rule _rule;
	private long _scn;

	public Oracle2SnapshotWorker(Rule rule, DataSource dbDataSource,
			DBWrapper destDB, long scn) {
		_rule = rule;
		_dbDataSource = dbDataSource;
		_destDBWrapper = destDB;
		_scn = scn;

	}

	@Override
	public void run() {
		try (Connection con = _dbDataSource.getConnection();) {
			JSONArray res = work(con, _rule);

			String coll = _rule.getCollectionName();
			if(coll == null){
				System.out.println(_rule);
				_destDBWrapper.createCollection(_rule.getCollectionName());
			}
			DBWrapperCollection collection = _destDBWrapper.getCollection(coll);

			collection.insertMany(res);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private JSONArray work(Connection con, Rule rule) throws SQLException {
		List<JSONArray> subcollections = new LinkedList<JSONArray>();
		for (Rule childRule : rule.getChildRules()) {
			JSONArray workDone = work(con, childRule);
			subcollections.add(workDone);
		}

		String sortingField = rule.getLinkSrc() == null ? rule.getIdFieldName()
				: rule.getLinkSrc();
		String sql = String.format(
				"select * from (%s) as of scn ? order by %s asc",
				rule.getSql(), sortingField);
		System.out.println(sql);
		try (PreparedStatement ps = con.prepareStatement(sql);) {
			ps.setLong(1, _scn);
			try (ResultSet rs = ps.executeQuery();) {
				JSONArray json = JsonConverter.convert(rs);
				
				List<Rule> childRules = rule.getChildRules();
				Iterator<Rule> iter = childRules.iterator();
				for (JSONArray subcol : subcollections) {
					join(json, subcol, iter.next());
				}
				
				JSONArray jsonArr = rearrange(json, rule.getLinkSrc(),
						rule.getCollectionName());
				
				System.out.println(jsonArr);
				return jsonArr;
			}
		}
	}

	/**
	 * join to json arrays
	 * 
	 * @param jsonArr
	 * @param subcol
	 * @param collectionName
	 */
	private void join(JSONArray jsonArr, JSONArray subcol, Rule rule) {
		System.out.println("CCCC" + subcol);
		int j = 0;
		for (int i = 0; i < jsonArr.size(); i++) {
			JSONObject jo = (JSONObject) jsonArr.get(i);
			if(j >= subcol.size())
				return;
			JSONArray potentialSubcol = (JSONArray) ((JSONObject) subcol.get(j))
					.get(rule.getCollectionName());
			System.out.println("DDD"+potentialSubcol);
			System.out.println("EEE "+rule.getCollectionName());
			System.out.println("FFF "+subcol.get(j));
			System.out.println("AAAA");
			if (checkIfShouldBeJoined(jo, potentialSubcol, rule)) {
				((JSONObject) jsonArr.get(i)).put(rule.getCollectionName(),
						potentialSubcol);
				j++;
			}
		}
	}

	private boolean checkIfShouldBeJoined(JSONObject jo,
			JSONArray subCollection, Rule rule) {
		System.out.println("B" + jo + "    " + subCollection);
		if (subCollection == null)
			return false;
		if (subCollection.isEmpty())
			return false;
		JSONObject subItem = (JSONObject) subCollection.get(0);
		Object linkSource = subItem.get(rule.getLinkSrc());
		System.out.println("QQQ" + subItem);
		System.out.println("QWWW" + rule.getLinkSrc());
		Object linkDest = jo.get(rule.getIdFieldName());
		System.out.println(linkDest + "____" + linkSource);
		if (linkDest.equals(linkSource)) {
			return true;
		}
		return false;
	}

	/**
	 * arranges all members with the same linkSource inside one array,
	 * to work with easily during recursion.
	 * @param json
	 * @param linkSrc
	 * @param collectionName
	 * @return
	 */
	private JSONArray rearrange(JSONArray json, String linkSrc,
			String collectionName) {
		JSONArray ja = new JSONArray();
		JSONObject currObject = null;

		Object linkSrcField = null;
		for (Object elem : json) {
			JSONObject jo = (JSONObject) elem;
			Object newLinkSrcField = jo.get(linkSrc);

			if (linkSrcField == null || !newLinkSrcField.equals(linkSrcField)) {
				currObject = new JSONObject();
				currObject.put(collectionName, new JSONArray());
				ja.add(currObject);
			}
			linkSrcField = jo.get(linkSrc);
			JSONArray tempJsonArray = (JSONArray) currObject
					.get(collectionName);
			tempJsonArray.add(elem);
		}

		return ja;
	}

}
