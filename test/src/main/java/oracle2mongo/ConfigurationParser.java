package oracle2mongo;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.commons.io.FileUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class ConfigurationParser {

	private String _confFile;
	private String _jdbcUrl;
	private String _mongoUrl;
	private String _mongoDBName;
	private ConcurrentMap<String, Collection<String>> _tableMap = new ConcurrentHashMap<>();

	public ConfigurationParser(String configurationFile, String jdbcUrl, String mongoUrl, String mongoDBName) {
		_confFile = configurationFile;
		_jdbcUrl = jdbcUrl;
		_mongoUrl = mongoUrl;
		_mongoDBName = mongoDBName;
	}

	public BlockingQueue<Runnable> getJobs() throws IOException, ParseException, SQLException {
		BlockingQueue<Runnable> jobs = new PriorityBlockingQueue<Runnable>();
		
		String confStr = FileUtils.readFileToString(new File(_confFile)).toUpperCase();
		System.out.println(confStr);
		JSONParser jp = new JSONParser();
		JSONArray jsonArr = (JSONArray) jp.parse(confStr);
		
		for(Object mapping:jsonArr){
			jobs.add(new OracleLoader2MongoWriter((JSONObject) mapping, _jdbcUrl, _mongoUrl, _mongoDBName, _tableMap));
		}
		
		return jobs;
	}
	
	public Map<String, Collection<String>> getSqlsForTable(){
		return _tableMap;
	}

}
