package oracle2;

import org.bson.Document;

import com.mongodb.client.MongoCollection;

public interface DBWrapper {

	void createCollection(String collectionName);

	DBWrapperCollection getCollection(String coll);

}
