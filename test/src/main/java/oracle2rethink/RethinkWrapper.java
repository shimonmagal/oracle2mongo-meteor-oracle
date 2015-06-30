package oracle2rethink;


import java.io.IOException;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.RethinkDBConnection;

import oracle2.DBWrapper;
import oracle2.DBWrapperCollection;

public class RethinkWrapper implements DBWrapper{

	
	
	private RethinkDB _r;
	private RethinkDBConnection _con;

	public RethinkWrapper(String host, int port, String dbName) {
		_r = RethinkDB.r;
		_con = _r.connect(host, port);
		_con.use(dbName);
	}

	@Override
	public void createCollection(String collectionName) {
		_r.table(collectionName).run(_con);
	}

	@Override
	public DBWrapperCollection getCollection(String coll) {
		return new RethinkWrapperCollection(_r.table(coll), _con);
	}

	@Override
	public void close() throws IOException {
		_con.close();
	}

}
