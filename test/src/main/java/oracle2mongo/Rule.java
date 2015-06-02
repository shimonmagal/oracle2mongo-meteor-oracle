package oracle2mongo;

import java.util.LinkedList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class Rule {
	private Rule _parentRule;
	private String _sql;
	private List<Rule> _childRules = new LinkedList<>();
	private String _linkSrc;
	private String _linkDest;
	private String _collectionName;
	private String _idField = "ID";
	
	public Rule(JSONObject ruleJson) {
		System.out.println(ruleJson);
		JSONObject ruleJO = (JSONObject) ruleJson.get("RULE");
		_collectionName = (String) ruleJson.get("COLLECTION");
		_sql = (String) ruleJO.get("SQL");
		_linkSrc = (String) ruleJO.get("LINK_SRC");
		//added support for primary key that is 
		if(ruleJO.get("ID") != null){
			_idField = (String) ruleJO.get("ID_FIELD");
		}
		JSONArray subrules = (JSONArray) ruleJO.get("SUBRULES");
		if(subrules != null){
			_childRules = new LinkedList<>();
			for(int i=0;i<subrules.size();i++){
				_childRules.add(new Rule((JSONObject) subrules.get(i), this));
			}
		}
	}

	public Rule(JSONObject jsonObject, Rule parentRule) {
		this(jsonObject);
		_parentRule = parentRule;
	}

	public Rule getParentRule() {
		return _parentRule;
	}
	
	public String getSql() {
		return _sql;
	}

	public List<Rule> getChildRules() {
		return _childRules;
	}


	public String getLinkSrc() {
		return _linkSrc;
	}

	public String getCollectionName() {
		return _collectionName;
	}
	
	public String getIdFieldName(){
		return _idField;
	}
	
}

