package bplustree;

import java.io.Serializable;
import java.util.Vector;

public class DictionaryPair implements Comparable<DictionaryPair>,Serializable{
	Key key;
	Vector<String> pageList;

	/**
	 * Constructor
	 * @param key: the key of the key-value pair
	 * @param value: the value of the key-value pair
	 */
	public DictionaryPair(Key key, String value) {
		this.key = key;
		this.pageList = new Vector<String>();
		this.pageList.add(value);
	}

	/**
	 * This is a method that allows comparisons to take place between
	 * DictionaryPair objects in order to sort them later on
	 * @param o
	 * @return
	 */
	@Override
	public int compareTo(DictionaryPair o) {
        Object thisValue = this.key;
        Object otherValue = o.key;

        // Check if both values are instances of Comparable
        if (thisValue instanceof Comparable && otherValue instanceof Comparable) {
            Comparable<Object> thisComparableValue = (Comparable<Object>) thisValue;
            Comparable<Object> otherComparableValue = (Comparable<Object>) otherValue;

            // Compare the values
            return thisComparableValue.compareTo(otherComparableValue);
        } else {
            throw new UnsupportedOperationException("Clustering key must implement Comparable");
        }
	}
	
	@Override
	public String toString() {
	    return "(" + key.toString() + ", " + pageList + ")";
	}

}