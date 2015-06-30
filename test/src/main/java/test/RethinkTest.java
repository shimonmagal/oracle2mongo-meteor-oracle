package test;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.JSQLParserException;
import oracle2.Oracle2Mongo;
import oracle2rethink.RethinkWrapper;

import org.json.simple.parser.ParseException;
import org.junit.Test;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.RethinkDBConnection;
import com.rethinkdb.model.MapObject;

public class RethinkTest {
	
	@Test
	public void test(){
		RethinkDB r = RethinkDB.r;
		RethinkDBConnection con = r.connect("192.168.1.13", 28015);
		con.use("test");
		Object z = new MapObject().with("c", "ddd");;
		List t = new LinkedList<>();
		t.add(z);
		
		Map<String, Object> dbObjects = new HashMap<>();
		dbObjects.put("a", t);
		r.
		r.table("new2").insert(dbObjects ).run(con);
		List<Map<String, Object>> t2 = r.table("new").run(con);
		System.out.println(t2);
	}
	
	/*@Test
	public static void test2() throws SQLException, ClassNotFoundException, IOException, ParseException, JSQLParserException, InterruptedException {
		Oracle2Mongo om = new Oracle2Mongo("jdbc:oracle:thin:test/test@localhost:1521:xe", new File("conf.json"), 22, new RethinkWrapper("192.168.1.13", 28015,  "test"));
		//om.replicateSnapshot();
		while(true){
		Thread.sleep(2000);
		om.replicateContinuent();
		}
	}*/

}
