package oracle2mongo;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class Rule {
	private Rule _parentRule;
	private String _sql;
	private Rule[] _childRules;
	private String _linkSrc;
	private String _linkDest;
	private String _collectionName;
	private String _idField = "ID";
	
	public Rule(JSONObject ruleJson) {
		JSONObject ruleJO = (JSONObject) ruleJson.get("RULE");
		_collectionName = (String) ruleJO.get("COLLECTION");
		_sql = (String) ruleJO.get("SQL");
		_linkSrc = (String) ruleJO.get("LINK_SRC");
		_linkSrc = (String) ruleJO.get("LINK_DEST");
		//added support for primary key that is 
		if(ruleJO.get("ID") != null){
			_idField = (String) ruleJO.get("ID_FIELD");
		}
		JSONArray subrules = (JSONArray) ruleJO.get("SUBRULES");
		_childRules = new Rule[subrules.size()];
		for(int i=0;i<subrules.size();i++){
			_childRules[i] = new Rule((JSONObject) subrules.get(i), this);
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

	public Rule[] getChildRules() {
		return _childRules;
	}


	public String getLinkSrc() {
		return _linkSrc;
	}

	public String getLinkDest() {
		return _linkDest;
	}

	public String getCollectionName() {
		return _collectionName;
	}
	
}

