package oracle2;

import java.util.List;
import java.util.Map;

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;


public interface DBWrapperCollection {

	public void deleteMany(Bson x);

	public void updateOne(Bson x, Document doc);

	public void updateOne(BsonDocument filterDocument,
			BsonDocument infoDocument);

	void insertMany(List<Map<String, Object>> docs);

}
