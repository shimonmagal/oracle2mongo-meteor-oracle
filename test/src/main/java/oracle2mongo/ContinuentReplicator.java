package oracle2mongo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ContinuentReplicator {

	private long _scn;
	private String _jdbcUrl;

	public ContinuentReplicator(long scn, String jdbcUrl) {
		_scn = scn;
		_jdbcUrl = jdbcUrl;
	}
	
	public void replicate() throws SQLException{
		Connection c = DriverManager.getConnection(_jdbcUrl);
		PreparedStatement ps = c.prepareStatement("select t.*, t.ora_rowscn from log order by t.ora_rowscn asc");
		ResultSet rs = ps.executeQuery();
		while(rs.next()){
			
		}
		
		
	}

}
