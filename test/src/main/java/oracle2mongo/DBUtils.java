package oracle2mongo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBUtils {
	
	public Connection getConnection(String jdbc) throws SQLException{
		return DriverManager.getConnection(jdbc);
	}

}
