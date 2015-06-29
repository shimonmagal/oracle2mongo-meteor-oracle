package oracle2mongo;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import oracle2.DBWrapper;
import oracle2.DBWrapperCollection;

public class MongoWrapper implements DBWrapper{

	private MongoDatabase _con;

	public MongoWrapper(String url, String db) {
		MongoClient mongoClient = new MongoClient(url);
		_con = mongoClient.getDatabase(db);
	}

	@Override
	public void createCollection(String collectionName) {
		_con.createCollection(collectionName);
	}

	@Override
	public DBWrapperCollection getCollection(String coll) {
		return new MongoWrapperCollection(_con.getCollection(coll));
	}

}
