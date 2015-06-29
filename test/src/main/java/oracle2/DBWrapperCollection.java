package oracle2;

import java.util.List;

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.result.UpdateResult;

public interface DBWrapperCollection {

	public void insertMany(List<Document> docs);

	public void deleteMany(Bson x);

	public void updateOne(Bson x, Document doc);

	public void updateOne(BsonDocument filterDocument,
			BsonDocument infoDocument);

}
