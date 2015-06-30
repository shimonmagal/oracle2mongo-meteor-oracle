package oracle2mongo;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.UpdateResult;

import oracle2.DBWrapperCollection;

public class MongoWrapperCollection implements DBWrapperCollection {

	private MongoCollection<Document> _coll;

	public MongoWrapperCollection(MongoCollection<Document> collection) {
		_coll = collection;
	}

	@Override
	public void deleteMany(Bson x) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateOne(Bson x, Document doc) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateOne(BsonDocument filterDocument, BsonDocument infoDocument) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void insertMany(List<Map<String, Object>> res) {
		List<Document> docs = new LinkedList<>();
		System.out.println("-----------=====");
		for (Object elem : res) {
			JSONObject jo = (JSONObject) elem;
			Document doc = Document.parse(((JSONArray) jo.get(coll)).get(0)
					.toString());
			docs.add(doc);
			System.out.println(doc);
		}

		
	}


}
