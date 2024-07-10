package Engine;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

//This is the table class which holds table info

public class Table implements Serializable {
	// Add attributes as needed

	private Vector<PageInfo> pageList;
	private int pageCounter;
	private String tableName;
	private transient Vector<IndexInfo> indexList; // These should be loaded from metadata file
	private transient Hashtable<String, String> htblColNameType; // This should be loaded from metadata file
	private transient String clusteringKeyName; // This should be loaded from metadata file

	public Table(String tableName) {
		pageList = new Vector<PageInfo>();
		pageCounter = 0;
		this.tableName = tableName;
	}
	
	public void loadMetaData() throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader("data/metadata.csv"));
        String line;
        
		htblColNameType = new Hashtable<String, String>();
		indexList = new Vector<IndexInfo>();

        // Read metadata for the specified table
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split(",");
            String table = parts[0];
            if (table.equals(tableName)) {
                String columnName = parts[1];
                String columnType = parts[2];
                boolean isClusteringKey = Boolean.parseBoolean(parts[3]);
                if (isClusteringKey) {
                	clusteringKeyName = columnName;
                }
                htblColNameType.put(columnName, columnType);
                String index = parts[4];
                if (!index.equals("null")) {
                	indexList.add(new IndexInfo(index, columnName));
                }
            }
        }
        reader.close();
	}
	
	// Method to validate columns against metadata
	public void validateColumns(Hashtable<String, Object> htblColNameValue) throws DBAppException {

	    // Check if all columns exist in metadata and values match the types specified
	    for (Map.Entry<String, Object> entry : htblColNameValue.entrySet()) {
	        String colName = entry.getKey();
	    	
	        // Check if column exists in metadata
	        if (!htblColNameType.keySet().contains(colName)) {
	            throw new DBAppException("Column '" + colName + "' does not exist in the table.");
	        }
	        
	        Object colValue = entry.getValue();
	        if(colValue == null) {
	        	throw new DBAppException("Value for column " + colName + " is null");
	        }
	        String expectedType = htblColNameType.get(colName);

	        // Check if value matches the expected type
	        if (!isValueOfType(colValue, expectedType)) {
	            throw new DBAppException("Value '" + colValue + "' for column '" + colName + "' does not match the expected type '" + expectedType + "'.");
	        }
	    }
	}
	
    // Method to check if a value is of the specified type
    private boolean isValueOfType(Object value, String type) {
        switch (type) {
            case "java.lang.Integer":
                return value instanceof Integer;
            case "java.lang.Double":
                return value instanceof Double;
            case "java.lang.String":
                return value instanceof String;
            // Add more cases for other types as needed
            default:
                return false;
        }
    }


	public String getTableName() {
		return tableName;
	}

	public Vector<PageInfo> getPageList() {
		return pageList;
	}
	
    // Method to retrieve the names of all pages before a specific index, including the index itself
    public Vector<String> getPagesBeforeIndex(int index) {
    	Vector<String> pages = new Vector<>();
        for (int i = 0; i <= index; i++) {
            pages.add(pageList.get(i).getPageName());
        }
        return pages;
    }

    // Method to retrieve the names of all pages after a specific index, including the index itself
    public Vector<String> getPagesAfterIndex(int index) {
    	Vector<String> pages = new Vector<>();
        for (int i = index; i < pageList.size(); i++) {
            pages.add(pageList.get(i).getPageName());
        }
        return pages;
    }

	// Adds the given page to the end of the array
	public void addPage(PageInfo pageInfo) throws DBAppException {
		pageList.add(pageInfo);
		try {
			FileAccess.writeTable(this);
		} catch (IOException e) {
			throw new DBAppException("Error writing table to memory: " + e.getMessage());
		}
	}

	public Vector<IndexInfo> getIndexList() {
		return indexList;
	}

	public void addIndex(IndexInfo indexName) throws DBAppException {
		indexList.add(indexName);
		try {
			FileAccess.writeTable(this);
		} catch (IOException e) {
			throw new DBAppException("Error writing table to memory: " + e.getMessage());
		}
	}

	public void deletePage() {
		// To be implemented
	}
	
	public Vector<String> getAllPageNames() {
		Vector<String> pageNames =  new Vector<String>(); 
		for (PageInfo pageInfo : pageList) {
			pageNames.add(pageInfo.getPageName());
		}
		return pageNames;
	}

	// Perform binary search to find the correct page to insert the tuple
	public int findPage(Object clusteringKeyValue) {
		PageInfo Comparator = new PageInfo(null, clusteringKeyValue, false);

		// Check if the last page has to be less than the first record in the next page
		if (Comparator.compareTo(pageList.get(pageList.size() - 1)) > 0) {
			// Tuple should be inserted at the end
			return pageList.size() - 1;
		}

		// Binary search for the correct page
		int index = Collections.binarySearch(pageList, Comparator);
		if (index < 0) {
			// Convert negative insertion point to actual index
			index = -index - 2;
			
			if (index == -1) {
				index ++;
			}
		}
		// Return the index of the page to insert the tuple
		return index;
	}

	public int getPageCounter() {
		return pageCounter;
	}

	public void incrementPageCounter() throws DBAppException {
		this.pageCounter++;
		try {
			FileAccess.writeTable(this);
		} catch (IOException e) {
			throw new DBAppException("Error writing table to memory: " + e.getMessage());
		}
	}

	public void updateInfo(int index, Page page) throws DBAppException {
		pageList.get(index).setMinValue(page.getTupleList().get(0).getClusteringKeyValue());
		pageList.get(index).setFull(page.isPageFull());
		// Write the updated table object back to memory
		try {
			FileAccess.writeTable(this);
		} catch (IOException e) {
			throw new DBAppException("Error writing table to memory: " + e.getMessage());
		}
	}

	public void updateIndices(Tuple tuple, String oldPageName, String newPageName) throws DBAppException {
		for (IndexInfo indexInfo : indexList) {
			Index index;
			String indexName = indexInfo.getIndexName();
			try {
				index = FileAccess.readIndex(indexName);
			} catch (IOException | ClassNotFoundException e) {
				throw new DBAppException("Error loading index " + indexName + ": " + e.getMessage());
			}

			// Update the index
			System.out.println(oldPageName);
			System.out.println(newPageName);
			System.out.println(tuple.getColumnValue(indexInfo.getColName()));
			index.update(tuple.getColumnValue(indexInfo.getColName()), oldPageName, newPageName);
		}
	}

	public void insertIntoIndices(Tuple tuple, String pageName) throws DBAppException {
		for (IndexInfo indexInfo : indexList) {
			Index index;
			String indexName = indexInfo.getIndexName();
			try {
				index = FileAccess.readIndex(indexName);
			} catch (IOException | ClassNotFoundException e) {
				throw new DBAppException("Error loading index " + indexName + ": " + e.getMessage());
			}

			// Insert into the index
			index.insert(tuple.getColumnValue(indexInfo.getColName()), pageName);
		}
	}

	public void deleteFromIndices(Tuple tuple, String pageName) throws DBAppException {
		for (IndexInfo indexInfo : indexList) {
			Index index;
			String indexName = indexInfo.getIndexName();
			try {
				index = FileAccess.readIndex(indexName);
			} catch (IOException | ClassNotFoundException e) {
				throw new DBAppException("Error loading index " + indexName + ": " + e.getMessage());
			}

			// Delete the entry from the index
			index.delete(tuple.getColumnValue(indexInfo.getColName()), pageName);
		}
	}

	public void createPage() throws DBAppException {
		// Increment the page counter
		this.incrementPageCounter();

		// Construct the unique page name using the table name and page counter
		String pageName = this.getTableName() + this.getPageCounter();

		// Create the file for the new page
		FileAccess.createPageFile(pageName);

		// Create a new PageInfo object for the page and add it to the table
		this.addPage(new PageInfo(pageName, null, false));

		// Create a new page object
		Page newPage = new Page(pageName);

		// Write the new page to memory
		try {
			FileAccess.writePage(newPage);
		} catch (IOException e) {
			throw new DBAppException("Error writing new page to memory: " + e.getMessage());
		}
	}

	public int calculateForwardDistance(int pageIndex) {
		int forwardDistance = 0;
		for (int i = pageIndex + 1; i < this.getPageList().size(); i++) {
			if (!this.getPageList().get(i).isFull()) {
				forwardDistance = i - pageIndex;
				break;
			}
		}
		return forwardDistance;
	}

	public int calculateBackwardDistance(int pageIndex) {
		int backwardDistance = 0;
		for (int i = pageIndex - 1; i >= 0; i--) {
			if (!this.getPageList().get(i).isFull()) {
				backwardDistance = pageIndex - i;
				break;
			}
		}
		return backwardDistance;
	}

	public void shiftTuplesForward(int pageIndex) throws DBAppException {
		// Get the page name
		String pageName = this.getPageList().get(pageIndex).getPageName();

		// Get the page from memory
		Page currentPage;
		try {
			currentPage = FileAccess.readPage(pageName);
		} catch (IOException | ClassNotFoundException e) {
			throw new DBAppException("Error loading page " + pageName + ": " + e.getMessage());
		}

		// Get the last tuple from the page
		Tuple lastTuple = currentPage.getTupleList().remove(currentPage.getTupleList().size() - 1);

		try {
			// Write the page back to memory
			FileAccess.writePage(currentPage);
		} catch (IOException e) {
			throw new DBAppException("Error removing tuple from page " + pageName + ": " + e.getMessage());
		}

		// Loop global variables
		int nextPageIndex = pageIndex + 1;
		boolean flag = false;

		do {
			// Get the next page name
			String nextPageName = this.getPageList().get(nextPageIndex).getPageName();

			// Get the next page from memory
			Page nextPage;
			try {
				nextPage = FileAccess.readPage(nextPageName);
			} catch (IOException | ClassNotFoundException e) {
				throw new DBAppException("Error loading page " + nextPageName + ": " + e.getMessage());
			}

			try {

				// Add the tuple to the beginning of the next page
				nextPage.getTupleList().add(0, lastTuple);
				this.updateIndices(lastTuple, pageName, nextPageName);
				
				// Is the page full (overflowing)
				flag = nextPage.isPageOverflowing();
				

				// Remove the last tuple if the page is full prior to insertion
				if (flag) {
					lastTuple = nextPage.getTupleList().remove(nextPage.getTupleList().size() - 1);
				}

				// Update minimum value and full flag
				this.updateInfo(nextPageIndex, nextPage);

				// Write the page back to memory
				FileAccess.writePage(nextPage);

			} catch (IOException e) {
				throw new DBAppException("Error adding tuple to page " + nextPageName + ": " + e.getMessage());
			}
			// Increment the next page index
			nextPageIndex++;
			pageName = nextPageName;
		} while (flag);
	}

	public void shiftTuplesBackward(int pageIndex) throws DBAppException {
		// Get the page name
		String pageName = this.getPageList().get(pageIndex).getPageName();

		// Get the page from memory
		Page currentPage;
		try {
			currentPage = FileAccess.readPage(pageName);
		} catch (IOException | ClassNotFoundException e) {
			throw new DBAppException("Error loading page " + pageName + ": " + e.getMessage());
		}

		// Get the first tuple from the page
		Tuple firstTuple = currentPage.getTupleList().get(0);

		try {
			// Remove the first tuple from the page
			currentPage.getTupleList().remove(0);

			// Update table info
			this.updateInfo(pageIndex, currentPage);

			// Write the page back to memory
			FileAccess.writePage(currentPage);
		} catch (IOException e) {
			throw new DBAppException("Error removing tuple from page " + pageName + ": " + e.getMessage());
		}

		// Loop global variables
		int prevPageIndex = pageIndex - 1;
		boolean flag = false;

		do {
			// Get the previous page name
			String prevPageName = this.getPageList().get(prevPageIndex).getPageName();

			// Get the previous page from memory
			Page prevPage;
			try {
				prevPage = FileAccess.readPage(prevPageName);
			} catch (IOException | ClassNotFoundException e) {
				throw new DBAppException("Error loading page " + prevPageName + ": " + e.getMessage());
			}

			try {

				// Add the tuple to the end of the previous page
				prevPage.getTupleList().add(firstTuple);
				
				// Update the indices
				this.updateIndices(firstTuple, pageName, prevPageName);

				flag = prevPage.isPageOverflowing();

				// Remove the first tuple if the page is full prior to insertion
				if (flag) {
					firstTuple = prevPage.getTupleList().remove(0);
				}

				// Update minimum value and full flag
				this.updateInfo(prevPageIndex, prevPage);

				// Write the page back to memory
				FileAccess.writePage(prevPage);


			} catch (IOException e) {
				throw new DBAppException("Error adding tuple to page " + prevPageName + ": " + e.getMessage());
			}
			// Decrement the previous page index
			prevPageIndex--;
			pageName = prevPageName;
		} while (flag);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (PageInfo pageInfo : pageList) {
			String pageName = pageInfo.getPageName();
			try {
				Page page = FileAccess.readPage(pageName);
				sb.append("Page Name: ").append(page.getPageName()).append("\n");
				sb.append(page.toString()).append("\n");
			} catch (IOException | ClassNotFoundException e) {
				sb.append("Error loading page ").append(pageName).append(": ").append(e.getMessage()).append("\n");
			}
		}
		return sb.toString();
	}

	public String getClusteringKeyName() {
		return clusteringKeyName;
	}

	public void setClusteringKeyName(String clusteringKeyName) {
		this.clusteringKeyName = clusteringKeyName;
	}

	public Hashtable<String, String> getHtblColNameType() {
		return htblColNameType;
	}

	public void setHtblColNameType(Hashtable<String, String> htblColNameType) {

		this.htblColNameType = htblColNameType;
	}

	public Vector<Vector<String>> searchWithIndices(Tuple tuple) throws DBAppException {
		
		Vector<Vector<String>> pageNames = new Vector<Vector<String>>();
		
		// For every column in the tuple
		for (String colName : tuple.getRecord().keySet()) {
			
			// get the column name
			Object ColValue = tuple.getColumnValue(colName);
			Index index;
			String indexName = null;
			
			// For every index in defined over the table
			for (IndexInfo indexInfo : indexList) {
				// If index is defined over that column
				if (indexInfo.getColName().equals(colName)) {
					// Get the index name
					indexName = indexInfo.getIndexName();
					break;
				}
			}
			
			// If the index name is not null
			if (indexName != null) {
				
				// load the index
				try {
					index = FileAccess.readIndex(indexName);
				} catch (IOException | ClassNotFoundException e) {
					throw new DBAppException("Error loading index " + indexName + ": " + e.getMessage());
				}
				// Add the results of the search to the table
				pageNames.add(new Vector<>(index.search(ColValue)));
			}
		}

		return pageNames;
	}
}
