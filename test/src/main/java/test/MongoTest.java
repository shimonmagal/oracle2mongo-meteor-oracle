package test;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import net.sf.jsqlparser.JSQLParserException;
import oracle2.Oracle2Mongo;
import oracle2mongo.MongoWrapper;

import org.json.simple.parser.ParseException;
import org.junit.Test;

/**
 * Hello world!
 *
 */
public class MongoTest 
{
	@Test
	public static void test() throws SQLException, ClassNotFoundException, IOException, ParseException, JSQLParserException, InterruptedException {
		Oracle2Mongo om = new Oracle2Mongo("jdbc:oracle:thin:test/test@localhost:1521:xe", new File("conf.json"), 22, new MongoWrapper("localhost:3001", "meteor"));
		//om.replicateSnapshot();
		while(true){
		Thread.sleep(2000);
		om.replicateContinuent();
		}
	}
}



