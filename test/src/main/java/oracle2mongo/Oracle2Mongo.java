package oracle2mongo;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.json.simple.parser.ParseException;

public class Oracle2Mongo {
	
	private String _jdbcUrl;
	private String _mongoUrl;
	private int _threadCount;
	private String _configurationFile;
	private String _mongoDB;

	public Oracle2Mongo(String jdbcUrl, String mongoUrl, int threadCount, String configurationFile, String mongoDB){
		_jdbcUrl = jdbcUrl;
		_mongoUrl = mongoUrl;
		_threadCount = threadCount;
		_configurationFile = configurationFile;
		_mongoDB = mongoDB;
	}
	
	public void replicate() throws IOException, ParseException, SQLException{
		//get the configuration to see how to replicate
		ConfigurationParser configParse = new ConfigurationParser(_configurationFile, _jdbcUrl, _mongoUrl, _mongoDB);
		BlockingQueue<Runnable> jobs = configParse.getJobs();
		
		BlockingQueue<Runnable> workers = new ArrayBlockingQueue<Runnable>(_threadCount);
		//execute threads to perform different jobs
		ThreadPoolExecutor tpe = new ThreadPoolExecutor(_threadCount, _threadCount, 10000, TimeUnit.SECONDS, workers );
		for(Runnable job:jobs){
			tpe.execute(job);	
		}
		
		tpe.shutdown();
		
	}

}
