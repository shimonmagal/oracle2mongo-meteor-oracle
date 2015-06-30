package oracle2mongo;

import java.io.Closeable;
import java.io.IOException;

import oracle2.DBWrapper;
import oracle2.DBWrapperCollection;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

public class MongoWrapper implements DBWrapper{

	private MongoDatabase _con;
	private MongoClient _mongoClient;

	public MongoWrapper(String url, String db) {
		_mongoClient = new MongoClient(url);
		_con = _mongoClient.getDatabase(db);
	}

	@Override
	public void createCollection(String collectionName) {
		_con.createCollection(collectionName);
	}

	@Override
	public DBWrapperCollection getCollection(String coll) {
		return new MongoWrapperCollection(_con.getCollection(coll));
	}

	@Override
	public void close() throws IOException {
		_mongoClient.close();
		
	}
	

}
