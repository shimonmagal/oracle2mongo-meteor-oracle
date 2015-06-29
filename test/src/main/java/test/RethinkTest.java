package test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.RethinkDBConnection;

public class RethinkTest {
	
	@Test
	public void test(){
		RethinkDB r = RethinkDB.r;
		RethinkDBConnection con = r.connect("192.168.1.13", 28015);
		con.use("test");
		Map<String, Object> dbObjects = new HashMap<>();
		dbObjects.put("a", "b");
		r.table("new").insert(dbObjects ).run(con);
		List<Map<String, Object>> t = r.table("new").run(con);
		System.out.println(t);
	}

}
