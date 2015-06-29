package oracle2mongo;

import java.util.List;

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.UpdateResult;

import oracle2.DBWrapperCollection;

public class MongoWrapperCollection implements DBWrapperCollection {

	private MongoCollection<Document> _coll;

	public MongoWrapperCollection(MongoCollection<Document> collection) {
		_coll = collection;
	}

	@Override
	public void insertMany(List<Document> docs) {
		_coll.insertMany(docs);
		
	}

	@Override
	public void deleteMany(Bson x) {
		_coll.deleteMany(x);
		
	}

	@Override
	public void updateOne(Bson x, Document doc) {
		_coll.updateOne(x, doc);
		
	}

	@Override
	public void updateOne(BsonDocument filterDocument,
			BsonDocument infoDocument) {
		_coll.updateOne(filterDocument, infoDocument);
	}

}
