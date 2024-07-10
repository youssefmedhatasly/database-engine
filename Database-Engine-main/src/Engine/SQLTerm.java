package Engine;

/** * @author Wael Abouelsaadat */

public class SQLTerm {

	public String _strTableName, _strColumnName, _strOperator;
	public Object _objValue;

	public SQLTerm() {

	}
	
	public SQLTerm(String tableName, String colName, String operator, Object objValue ) {
		_strTableName = tableName;
		_strColumnName = colName;
		_strOperator = operator;
		_objValue = objValue;
	}

	public String getTableName() {
		return _strTableName;
	}

	public String getColName() {
		return _strColumnName;
	}

	public String getOperator() {
		return _strOperator;
	}
	
	public Object getObjValue() {
		return _objValue;
	}
}