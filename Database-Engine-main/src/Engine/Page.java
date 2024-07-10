package Engine;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;

public class Page implements Serializable{
	
	private String pageName;
	private int maximumTuples;
	private Vector<Tuple> tupleList;

	public Vector<Tuple> getTupleList() {
		return tupleList;
	}

	
	public Page(String pageName) {
		this.pageName = pageName;
		tupleList = new Vector<Tuple>();
		
		// The following code gets the maximum row number from the config file
        Properties properties = new Properties();
        try {
            // Load the DBApp.config file from the resources directory
        	FileInputStream  inputStream = new FileInputStream("config/DBApp.config");
            properties.load(inputStream);
            inputStream.close();

            // Get the value of MaximumRowsCountinPage property
            String maxRowsCount = properties.getProperty("MaximumRowsCountinPage");

            // Convert the value to integer and assign it to maximumTuples
            maximumTuples = Integer.parseInt(maxRowsCount);
        } catch (IOException | NumberFormatException e) {
            // Handle any errors while reading or parsing the property value
            e.printStackTrace();
        }
	}
	
	public String getPageName() {
		return pageName;
		
	}
	
	public boolean isPageFull(){
		return this.tupleList.size() >= maximumTuples;
		// Check if the page is full
	}
	
	public boolean isPageOverflowing(){
		return this.tupleList.size() > maximumTuples;
		// Check if the page is full
	}
	
	public boolean isPageEmpty(){
		return this.tupleList.size() == 0;
		// Check if the page is empty
	}
	
	public int findTuple(Tuple tuple) {
		return Collections.binarySearch(tupleList, tuple);
	}
	
	public void addTuple(Tuple tuple) throws DBAppException {

	    // If tupleList is empty or the new tuple is greater than the last tuple, add it at the end
	    if (tupleList.isEmpty() || tuple.compareTo(tupleList.lastElement()) > 0) {
	        tupleList.add(tuple);
	    }
	    else {
		    // Perform binary search to find the position to insert the tuple
		    int index = findTuple(tuple);
		
		    // Check if the clustering key already exists
		    if (index >= 0) {
		        throw new DBAppException("Tuple with clustering key " + tuple.getClusteringKeyValue() + " already exists.");
		    }
		
		    // Insert the tuple at the appropriate position
		    tupleList.add(-index - 1, tuple);
	    }
	    // Write updated page back to memory
	    try {
	        FileAccess.writePage(this);
	    } catch (IOException e) {
	        throw new DBAppException("Error writing page to memory: " + e.getMessage());
	    }
	}

	//Removes a given tuple from the page
	public Vector<Tuple> deleteTuplesByColumn(Hashtable<String, Object> columnSubset) throws DBAppException {
		Vector<Tuple> deletedTuples = new Vector<Tuple>();
		
	    Iterator<Tuple> tupleIterator = tupleList.iterator();
	    boolean tuplesDeleted = false;

	    while (tupleIterator.hasNext()) {
	        Tuple tuple = tupleIterator.next();
	        boolean tupleMatches = true;

	        // Check if the tuple matches the values in the subset of columns
	        for (String columnName : columnSubset.keySet()) {
	            if (!tuple.getRecord().containsKey(columnName) ||
	                !tuple.getRecord().get(columnName).equals(columnSubset.get(columnName))) {
	                tupleMatches = false;
	                break; // No need to check further if one column value does not match
	            }
	        }

	        // If the tuple matches, remove it from the tupleList
	        if (tupleMatches) {
	            tupleIterator.remove();
	            deletedTuples.add(tuple);
	            tuplesDeleted = true;
	        }
	    }

	    // Write the modified page back to memory
	    if (tuplesDeleted) {
	        try {
	            FileAccess.writePage(this);
	        } catch (IOException e) {
	            throw new DBAppException("Error writing page to memory: " + e.getMessage());
	        }
	    }

	    return deletedTuples; // Return true if the page is empty after deletion
	}
	
		
	// Convert the page to a string
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Tuple tuple : tupleList) {
            sb.append(tuple.toString()).append("\n");
        }
        return sb.toString();
    }
}
