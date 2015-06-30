package oracle2;

import java.io.Closeable;

public interface DBWrapper extends Closeable {

	void createCollection(String collectionName);

	DBWrapperCollection getCollection(String coll);

}
