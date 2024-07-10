package Engine;

import java.io.Serializable;
import java.util.Hashtable;

public class Tuple implements Serializable, Comparable<Tuple> {
    private Hashtable<String, Object> record;
    private String strClusteringKey;

    public Tuple(Hashtable<String, Object> record, String strClusteringKey) {
        this.strClusteringKey = strClusteringKey;
        this.record = record;
    }

    // Get the value of the clustering key
    public Object getClusteringKeyValue() {
        return record.get(strClusteringKey);
    }
    
    public Hashtable<String,Object> getRecord(){
    	return this.record;
    }
    
    // Get the value of a specific column
    public Object getColumnValue(String columnName) {
        return record.get(columnName);
    }

    // Implement compareTo method to compare tuples based on clustering key
	@Override
	public int compareTo(Tuple o) {
		// TODO Auto-generated method stub
		switch(this.getClusteringKeyValue().getClass().getSimpleName()) {
		case "String":
			String thisKeyString = (String) this.getClusteringKeyValue();
			String otherKeyString = (String)o.getClusteringKeyValue();
			return (thisKeyString).compareTo(otherKeyString);
		case "Integer":
			Integer thisKeyInteger = (Integer) this.getClusteringKeyValue();
			Integer otherKeyInteger = (Integer)o.getClusteringKeyValue();
			return (thisKeyInteger).compareTo(otherKeyInteger);
		case "Double":
			Double thisKeyDouble = (Double) this.getClusteringKeyValue();
			Double otherKeyDouble = (Double)o.getClusteringKeyValue();
			return (thisKeyDouble).compareTo(otherKeyDouble);
		}
		return 0;
	}

    // Convert the tuple to a String
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (String key : record.keySet()) {
            sb.append(record.get(key)).append(",");
        }
        if (sb.length() > 0) {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }
}
