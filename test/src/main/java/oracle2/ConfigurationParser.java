package oracle2;

import java.util.LinkedList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class ConfigurationParser {

	private String _confStr;

	public ConfigurationParser(String configurationString) {
		_confStr = configurationString;
	}

	public List<Rule> parse() throws ParseException {
		LinkedList<Rule> rules = new LinkedList<Rule>();
		JSONParser jp = new JSONParser();
		JSONArray jsonArr = (JSONArray) jp.parse(_confStr);
		
		for (Object mapping : jsonArr) {
			rules.add(calcRule((JSONObject) mapping));
		}

		return rules;
	}
	
	private Rule calcRule(JSONObject ruleJson) {
		return new Rule(ruleJson);
	}
}
