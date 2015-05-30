package test.test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class LogEvent {
	
	public static enum OPERATION {
		INSERT, UPDATE, DELETE
	}



	public static String _tableName;
	public static Set<Long> _ids = new HashSet<Long>();
	public static Set<String> _fields = new HashSet<String>();
	public static long _scn;
	public OPERATION _op;
	

	
	public LogEvent(String table, long scn, String fields, int op, String ids) {
		//tanle name:
		_tableName = table;
		
		//scn:
		_scn = scn;
		
		//fields:
		String[] fieldsSplit = fields.split("[\\s]*,[\\s]*");
		_fields.addAll(Arrays.asList(fieldsSplit));
		
		//op:
		switch(op){
			case 0:
				_op = OPERATION.INSERT; break;
			case 1:
				_op = OPERATION.UPDATE; break;
			default:
				_op = OPERATION.DELETE; break;
		}
		
		//ids:
		String[] idsSplit = ids.split("[\\s]*,[\\s]*");
		for(String idStr:idsSplit){
			_ids.add(Long.parseLong(idStr));
		}
		
	}

}

