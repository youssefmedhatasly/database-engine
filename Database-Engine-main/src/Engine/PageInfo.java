package Engine;

import java.io.Serializable;

import bplustree.Key;

public class PageInfo implements Comparable<PageInfo>, Serializable{
	
	private String pageName;
	private Object minValue;
	private boolean isFull;
	
	public PageInfo(String pageName, Object minValue, boolean isFull) {
		this.pageName = pageName;
		this.minValue = minValue;
		this.isFull = isFull;
	}
	
	public String getPageName() {
		return pageName;
	}
	
	public Object getMinValue() {
		return minValue;
	}
	
	public void setMinValue(Object minValue) {
		this.minValue = minValue;
	}
	
	public boolean isFull() {
		return isFull;
	}
	
	public void setFull(boolean isFull) {
		this.isFull = isFull;
	}

	@Override
	public int compareTo(PageInfo o) {
		if(o.getMinValue() == null) {
			return 1;
		}
		
		switch(minValue.getClass().getSimpleName()) {
		case "String":
			String thisKeyString = (String) this.minValue;
			String otherKeyString = (String)o.minValue;
			return (thisKeyString).compareTo(otherKeyString);
		case "Integer":
			Integer thisKeyInteger = (Integer) this.minValue;
			Integer otherKeyInteger = (Integer)o.minValue;
			return (thisKeyInteger).compareTo(otherKeyInteger);
		case "Double":
			Double thisKeyDouble = (Double) this.minValue;
			Double otherKeyDouble = (Double)o.minValue;
			return (thisKeyDouble).compareTo(otherKeyDouble);
		}
		return 0;
	}
	
	@Override
	public String toString() {
	    return "{" +
	            "pageName='" + pageName + '\'' +
	            ", minValue=" + minValue +
	            ", isFull=" + isFull +
	            '}';
	}

}
