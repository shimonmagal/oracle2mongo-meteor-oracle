package oracle2mongo;

public class ContinuentReplicator {

	private long _scn;
	private String _jdbcUrl;

	public ContinuentReplicator(long scn, String jdbcUrl) {
		_scn = scn;
		_jdbcUrl = jdbcUrl;
	}

}
