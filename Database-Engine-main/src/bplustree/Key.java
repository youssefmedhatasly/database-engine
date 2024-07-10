package bplustree;

import java.io.Serializable;

public class Key implements Comparable<Key>, Serializable{
	Object key;
	
	public Key(Object key) {
		this.key = key;
	}
	
	

	@Override
	public int compareTo(Key o) {
		// TODO Auto-generated method stub
		switch(key.getClass().getSimpleName()) {
		case "String":
			String thisKeyString = (String) this.key;
			String otherKeyString = (String)o.key;
			return (thisKeyString).compareTo(otherKeyString);
		case "Integer":
			Integer thisKeyInteger = (Integer) this.key;
			Integer otherKeyInteger = (Integer)o.key;
			return (thisKeyInteger).compareTo(otherKeyInteger);
		case "Double":
			Double thisKeyDouble = (Double) this.key;
			Double otherKeyDouble = (Double)o.key;
			return (thisKeyDouble).compareTo(otherKeyDouble);
		}
		return 0;
	}
	
	@Override
	public String toString() {
		return this.key.toString();
	}
	
}
