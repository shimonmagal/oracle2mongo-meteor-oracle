package test.test;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class Parser {

	public static void main(String[] args) throws IOException, ParseException {
		
		String str = FileUtils.readFileToString(new File("conf.json"));
		JSONArray jp = (JSONArray) new JSONParser().parse(str);
		analyze(jp);
	}

	private static void analyze(JSONArray jp) {
		if(jp == null)
			return;
		for(Object elem:jp){
			JSONObject jsonRule = ((JSONObject)elem);
			String collectionName = (String) jsonRule.get("collection");
			
			JSONObject queryDetails = (JSONObject) jsonRule.get("query");
			System.out.println(queryDetails.get("sql"));
			System.out.println(queryDetails.get("linkSrc"));
			System.out.println(queryDetails.get("linkDest"));
			JSONArray subqueries = (JSONArray) queryDetails.get("subqueries");
			analyze(subqueries);
		}

		
	}

}
