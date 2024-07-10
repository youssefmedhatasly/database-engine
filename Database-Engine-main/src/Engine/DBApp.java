package Engine;

/** * @author Wael Abouelsaadat */

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;

public class DBApp {

	public DBApp() {

	}

	// this does whatever initialization you would like
	// or leave it empty if there is no code you want to
	// execute at application startup
	public void init() {

	}

	// following method creates one table only
	// strClusteringKeyColumn is the name of the column that will be the primary
	// key and the clustering column as well. The data type of that column will
	// be passed in htblColNameType
	// htblColNameValue will have the column name as key and the data
	// type as value
	public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType) throws DBAppException {

		// To be implemented loading relevant data to metadata.csx

	    // Create a new Table object
	    Table table = new Table(strTableName);
	    
	    try {
			MetadataManager.recordTableMetadata(strTableName,htblColNameType,strClusteringKeyColumn);
		} catch (IOException e) {
			throw new DBAppException("Error loadiong from metadata file");
		}

	    // Create the corresponding file for the table
	    try {
	        FileAccess.createTableFile(strTableName);
	    } catch (DBAppException e) {
	        throw new DBAppException("Error creating table file: " + e.getMessage());
	    }

	    // Save the Table object to the file
	    try {
	        FileAccess.writeTable(table);
	    } catch (IOException e) {
	        throw new DBAppException("Error writing table to memory: " + e.getMessage());
	    }
	}

	// following method creates a B+tree index
	public void createIndex(String strTableName,
			String strColName,
			String strIndexName) throws DBAppException {

		// To be implemented adding the index information to the metadata file

		// Create a new Index object
		Index newIndex = new Index(strIndexName, 3); // Assuming 3 is the order of the B+ tree

		// Load the table with the specified name from memory
		Table table;
		try {
			table = FileAccess.readTable(strTableName);
		} catch (IOException | ClassNotFoundException e) {
			throw new DBAppException("Error loading table " + strTableName + ": " + e.getMessage());
		}
		
		try {
			table.loadMetaData();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new DBAppException("Error loading data from to metadata file: " + e.getMessage());
		}
		
		try {
			MetadataManager.recordIndexMetadata(strTableName, strColName, strIndexName);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new DBAppException("Error loading index into metadata file");
		}

		// Iterate over each page in the table's page list
		for (PageInfo pageInfo : table.getPageList()) {
			String pageName = pageInfo.getPageName();

			// Load the page from memory
			Page page;
			try {
				page = FileAccess.readPage(pageName);
			} catch (IOException | ClassNotFoundException e) {
				throw new DBAppException("Error loading page " + pageName + ": " + e.getMessage());
			}

			// Iterate over each tuple in the page
			for (Tuple tuple : page.getTupleList()) {
				// Extract the particular column value to build the index on
				Object columnValue = tuple.getColumnValue(strColName);

				// Add the column's value and the page to the index
				newIndex.insert(columnValue, pageName);
			}
		}

		// Write the index to the index file
		try {
			FileAccess.writeIndex(newIndex);
		} catch (IOException e) {
			throw new DBAppException("Error writing index to metadata file: " + e.getMessage());
		}
		
		table.addIndex(new IndexInfo(strIndexName, strColName));
	}

	// following method inserts one row only.
	// htblColNameValue must include a value for the primary key
	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		// Load the table object from memory
		Table table;
		try {
			table = FileAccess.readTable(strTableName);
		} catch (IOException | ClassNotFoundException e) {
			throw new DBAppException("Error loading table: " + e.getMessage());
		}
		
		try {
			table.loadMetaData();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new DBAppException("Error loading data from to metadata file: " + e.getMessage());
		}
		
		table.validateColumns(htblColNameValue);
		
		String primaryKeyColumnName = table.getClusteringKeyName();

		// Identify correct page for insertion
		Tuple tuple = new Tuple(htblColNameValue, primaryKeyColumnName);
		
		if (tuple.getClusteringKeyValue() == null) {
			throw new DBAppException("Clustering key field is missing");
		}

		if (table.getPageList().isEmpty()) {
			table.createPage();
		}

		int pageIndex = table.findPage(tuple.getClusteringKeyValue());
		String pageName = table.getPageList().get(pageIndex).getPageName();

		// Load page from memory
		Page page;
		try {
			page = FileAccess.readPage(pageName);
		} catch (IOException | ClassNotFoundException e) {
			throw new DBAppException("Error loading page: " + e.getMessage());
		}
		
		// Insert tuple into the page first
		try {
			page.addTuple(tuple);
			table.insertIntoIndices(tuple, pageName);
		} catch (DBAppException e) {
			throw new DBAppException("Error adding tuple to page: " + e.getMessage());
		}
		
		// Then check if page is overflowing and handle logic accordingly
		if (page.isPageOverflowing()) {
			// Calculate distances
			int forwardDistance = table.calculateForwardDistance(pageIndex);
			int backwardDistance = table.calculateBackwardDistance(pageIndex);
			
			
			// If no space found, add new page
			if (forwardDistance + backwardDistance == 0) {
				table.createPage();
				table.shiftTuplesForward(pageIndex);
			} else if ((forwardDistance != 0) && (backwardDistance == 0 || forwardDistance < backwardDistance)) {
				// Determine shift direction and perform necessary shifts
				table.shiftTuplesForward(pageIndex);
				
			} else {
				table.shiftTuplesBackward(pageIndex);
			}
		}
		
		table.updateInfo(pageIndex, page);
		
		System.out.println(table.getPageList());
		System.out.print(table);
	}

	// following method updates one row only
	// htblColNameValue holds the key and new value
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	// following method updates one row only
	// htblColNameValue holds the key and new value
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	public void updateTable(String strTableName,
			String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {
		// Load the table object from memory
		Table table;
		try {
			table = FileAccess.readTable(strTableName);
		} catch (IOException | ClassNotFoundException e) {
			throw new DBAppException("Error loading table " + strTableName + ": " + e.getMessage());
		}
		
		try {
			table.loadMetaData();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new DBAppException("Error loading data from to metadata file: " + e.getMessage());
		}
		
		table.validateColumns(htblColNameValue);
		
		
		String clusteringKeyName = table.getClusteringKeyName();
		String clusteringKeyType = table.getHtblColNameType().get(clusteringKeyName); // Get the clustering key type
		
		for (Map.Entry<String, Object> entry : htblColNameValue.entrySet()) {
	        String colName = entry.getKey();
	        if (colName.equals(clusteringKeyName)) {
	        	throw new DBAppException("Clustering key should not be provided");
	        }
		}

		// Type-cast strClusteringKeyValue based on the clustering key type
		Object clusteringKeyValue;
		switch (clusteringKeyType) {
			case "java.lang.Integer":
				clusteringKeyValue = Integer.parseInt(strClusteringKeyValue);
				break;
			case "java.lang.Double":
				clusteringKeyValue = Double.parseDouble(strClusteringKeyValue);
				break;
			case "java.lang.String":
				clusteringKeyValue = strClusteringKeyValue;
				break;
			default:
				throw new DBAppException("Unsupported clustering key type: " + clusteringKeyType);
		}

		// Find the index of the page where the record resides
		int pageIndex = table.findPage(clusteringKeyValue);

		// Get the name of the page
		String pageName = table.getPageList().get(pageIndex).getPageName();

		// Load the page from memory
		Page page;
		try {
			page = FileAccess.readPage(pageName);
		} catch (IOException | ClassNotFoundException e) {
			throw new DBAppException("Error loading page " + pageName + ": " + e.getMessage());
		}

		// Create the search tuple with the clustering key name
		Tuple searchTuple = new Tuple(new Hashtable<>(), clusteringKeyName);
		searchTuple.getRecord().put(clusteringKeyName, clusteringKeyValue); // Insert the clustering key value

		int tupleIndex = page.findTuple(searchTuple);
		if (tupleIndex < 0) {
			throw new DBAppException("Record with clustering key " + strClusteringKeyValue + " does not exist.");
		}

		// Get the tuple from the page
		Tuple tupleToUpdate = page.getTupleList().get(tupleIndex);

		// Remove the old tuple from the indices
		table.deleteFromIndices(tupleToUpdate, pageName);

		// Update the tuple's values with the new ones
		for (String columnName : htblColNameValue.keySet()) {
			Object newValue = htblColNameValue.get(columnName);
			tupleToUpdate.getRecord().put(columnName, newValue);
		}

		page.getTupleList().set(tupleIndex, tupleToUpdate);

		// Rewrite the page to memory
		try {
			FileAccess.writePage(page);
		} catch (IOException e) {
			throw new DBAppException("Error writing page " + pageName + " to memory: " + e.getMessage());
		}

		// Add the updated tuple to the indices
		table.insertIntoIndices(tupleToUpdate, pageName);
		
		// Update table info
		table.updateInfo(pageIndex, page);
		
		System.out.println(table.getPageList());
		System.out.print(table);
	}

	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search
	// to identify which rows/tuples to delete.
	// htblColNameValue enteries are ANDED together
	public void deleteFromTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException {	    // Load the table object from memory
	    Table table;
	    try {
	        table = FileAccess.readTable(strTableName);
	    } catch (IOException | ClassNotFoundException e) {
	        throw new DBAppException("Error loading table " + strTableName + ": " + e.getMessage());
	    }
	    
		try {
			table.loadMetaData();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new DBAppException("Error loading data from to metadata file: " + e.getMessage());
		}
		
		table.validateColumns(htblColNameValue);


	    // Check if the clustering key is provided
	    if (htblColNameValue.containsKey(table.getClusteringKeyName())) {
	        // Retrieve the clustering key value
	        Object clusteringKeyValue = htblColNameValue.get(table.getClusteringKeyName());
	        
	        // Find the page where the record resides
	        int pageIndex = table.findPage(clusteringKeyValue);
	        
	        // Load the page from memory
	        Page page;
	        String pageName = table.getPageList().get(pageIndex).getPageName();
	        try {
	            page = FileAccess.readPage(pageName);
	        } catch (IOException | ClassNotFoundException e) {
	            throw new DBAppException("Error loading page: " + e.getMessage());
	        }
	        
	        // Create a search tuple using the clustering key value
	        Tuple searchTuple = new Tuple(new Hashtable<>(), table.getClusteringKeyName());
	        searchTuple.getRecord().put(table.getClusteringKeyName(), clusteringKeyValue);
	        
	        // Find the tuple if it exists
	        int tupleIndex = page.findTuple(searchTuple);
	        
	        // If tuple does not exist
			if (tupleIndex <0) {
				throw new DBAppException("Tuple not found");
			}
	        
			// Remove tuple
	        Tuple tuple = page.getTupleList().remove(tupleIndex);
	        
		    try {
		        // Write the updated page back to memory
		        FileAccess.writePage(page);
		    } catch (IOException e) {
		    	throw new DBAppException("Tuple with clustering key value '" + tuple.getClusteringKeyValue() + "' not found.");
		    }
	        
	        // If the page is empty, remove it from the table
	        if (page.isPageEmpty()) {
	            table.getPageList().remove(pageIndex);
	            FileAccess.deletePageFile(pageName);
	        }
	        else {
				// Update table info
				table.updateInfo(pageIndex, page);
	        }
	        
	        // Update the index
	        table.deleteFromIndices(tuple, pageName);
	        

	    } else {
	    	
	    	Vector<Vector<String>> pageNames = table.searchWithIndices(new Tuple(htblColNameValue, ""));
	    	// Check if the indexList is not empty
	    	if (!table.getIndexList().isEmpty() && !pageNames.isEmpty()) {
	            // Use indices to find relevant pages
	            
	            Vector<String> intersection = findIntersection(pageNames);
	            
	            // Delete tuples from the intersecting pages
	            for (String pageName : intersection) {
	                Page page;
	                try {
	                    page = FileAccess.readPage(pageName);
	                } catch (IOException | ClassNotFoundException e) {
	                    throw new DBAppException("Error loading page: " + e.getMessage());
	                }
		            
	                // If a tuple was deleted
	                if (deleteTuplesByColumn(table, page, htblColNameValue)) {
	                    
	                	// Find index of page
		                Comparator<PageInfo> pageNameComparator = Comparator.comparing(PageInfo::getPageName);
		                int pageIndex = Collections.binarySearch(table.getPageList(), new PageInfo(pageName, null, false), pageNameComparator);
	                	
		                // If the page is empty, remove it from the table
		                if (page.isPageEmpty()) {
		                	table.getPageList().remove(pageIndex);
		                	FileAccess.deletePageFile(pageName);
		                }
		                else {
		                	// Update table info
		                	table.updateInfo(pageIndex, page);
		                }
	                
	                }
	            }
	        } else {
	    	    // Perform linear search and delete relevant tuples
	    	    Iterator<PageInfo> iterator = table.getPageList().iterator();
	    	    int pageIndex = 0;
	    	    while (iterator.hasNext()) {
	    	        PageInfo pageInfo = iterator.next();
	    	        Page page;
	    	        try {
	    	            page = FileAccess.readPage(pageInfo.getPageName());
	    	        } catch (IOException | ClassNotFoundException e) {
	    	            throw new DBAppException("Error loading page: " + e.getMessage());
	    	        }
	    	        
	                boolean tuplesDeleted = deleteTuplesByColumn(table, page, htblColNameValue);
	    	        
	    	        // If the page is empty, remove it from the table
	    	        if(tuplesDeleted) {
		    	        if (page.isPageEmpty()) {
		    	            iterator.remove();
		    	            FileAccess.deletePageFile(page.getPageName());
		    	        } else {
		    	        	table.updateInfo(pageIndex, page);
		    	        }
	    	        }

	    	        pageIndex++;
	    	    }
	        }
	        // Update the table object in memory
	        try {
	            FileAccess.writeTable(table);
	        } catch (IOException e) {
	            throw new DBAppException("Error writing table to memory: " + e.getMessage());
	        }
	    }
		System.out.println(table.getPageList());
		System.out.print(table);
	}
	
	
	//Removes a given tuple from the page
	public boolean deleteTuplesByColumn(Table table, Page page, Hashtable<String, Object> columnSubset) throws DBAppException {
		
	    Iterator<Tuple> tupleIterator = page.getTupleList().iterator();
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

	        // If the tuple matches, remove it from the tupleList and update index
	        if (tupleMatches) {
	            tupleIterator.remove();
	            tuplesDeleted = true;
	            
		        for (String colName : tuple.getRecord().keySet()) {
					Index index;
					String indexName = null;
					for (IndexInfo indexInfo : table.getIndexList()) {
						if (indexInfo.getColName().equals(colName)) {
							indexName = indexInfo.getIndexName();
							break;
						}
					}
					
					if (indexName != null) {
						try {
							index = FileAccess.readIndex(indexName);
						} catch (IOException | ClassNotFoundException e) {
							throw new DBAppException("Error loading index " + colName + ": " + e.getMessage());
						}
		                index.delete(tuple.getColumnValue(colName), page.getPageName());
					}
		        }
	        }
	    }

	    // Write the modified page back to memory
	    if (tuplesDeleted) {
	        try {
	            FileAccess.writePage(page);
	        } catch (IOException e) {
	            throw new DBAppException("Error writing page to memory: " + e.getMessage());
	        }
	    }
	    
	    return tuplesDeleted;
	}
	
	private Vector<String> findIntersection(Vector<Vector<String>> pageNames) {
		Vector<String> intersection = new Vector<String>();
		
		if (pageNames.size() != 0) {
			intersection = pageNames.get(0);
			
			for(int i = 1; i < pageNames.size();i++) {
				intersection.retainAll(pageNames.get(i));
			}
		}
		return intersection;
	}


	public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
			String[] strarrOperators) throws DBAppException {
		
		// pageNames is a list of vectors of pages representing the return results of searching with the index
		// If an index does not exist for the particular column we find null instead
		ArrayList<Vector<String>> pageNames = new ArrayList<Vector<String>>(arrSQLTerms.length);
		ArrayList<String> operatorList = new ArrayList<String>(Arrays.asList(strarrOperators));

		if(arrSQLTerms.length == 0 || (arrSQLTerms.length - strarrOperators.length != 1)) {
			throw new DBAppException("Invalid search criteria");
		}
		else {
			String tableName = arrSQLTerms[0].getTableName();
			
			// load table from memory
			Table table;
			try {
				table = FileAccess.readTable(tableName);
			} catch (IOException | ClassNotFoundException e) {
				throw new DBAppException("Error loading table " + tableName + ": " + e.getMessage());
			}
			
			// load metadata info to table
			try {
				table.loadMetaData();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				throw new DBAppException("Error loading data from to metadata file: " + e.getMessage());
			}
			
			// validate search terms
			Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
			for(int i =0; i<arrSQLTerms.length; i++) {
				htblColNameValue.put(arrSQLTerms[i].getColName(), arrSQLTerms[i].getObjValue());
			}
			table.validateColumns(htblColNameValue);

			
			// Populate pageNames
			for(int i=0; i<arrSQLTerms.length; i++) {
				String colName = arrSQLTerms[i].getColName();
				Object colValue = arrSQLTerms[i].getObjValue();
				String operator = arrSQLTerms[i].getOperator();
				
				if (operator.equals("!=")) {
					pageNames.add(i, null);
					continue;
				}
				else {
					// If the column is the clustering key
					if(colName.equals(table.getClusteringKeyName())) {
						// Get the page using binary search
						int pageIndex = table.findPage(colValue);
						Vector<String> clusteringKeyPages = new Vector<String>();
						switch(operator) {
							case "=":
								String pageName = table.getPageList().get(pageIndex).getPageName();
								clusteringKeyPages.add(pageName);
								break;
							case ">":
							case ">=":
								clusteringKeyPages = table.getPagesAfterIndex(pageIndex);
								break;
							case "<":
							case "<=":
								clusteringKeyPages = table.getPagesBeforeIndex(pageIndex);
								break;
							default:
								throw new DBAppException("Invalid operation type");
						}
						pageNames.add(i,clusteringKeyPages);
					}
					else {
						// Retrieve the index built on the column if any
						boolean isIndex = false;
						for (IndexInfo indexInfo : table.getIndexList()) {
							if (indexInfo.getColName().equals(colName)) {
								Index index;
								try {
									index = FileAccess.readIndex(indexInfo.getIndexName());
								} catch (IOException | ClassNotFoundException e) {
									throw new DBAppException("Error loading index " + indexInfo.getIndexName() + ": " + e.getMessage());
								}
								Vector<String> searchResult = new Vector<String>();
								switch(operator) {
								case "=":
									searchResult = index.search(colValue);
									break;
								case ">":
								case ">=":
								case "<":
								case "<=":
									searchResult = index.searchWithBounds(colValue, operator);
									break;
								default:
									throw new DBAppException("Invalid operation type");
								}
								pageNames.add(i,searchResult);
								isIndex = true;
								break;
							}
						}
						if(!isIndex) {
							pageNames.add(i, null);
						}
					}
				}
			}
			System.out.println(pageNames);
			
			// Loop for AND operation
			int index;
			while ((index = operatorList.indexOf("AND")) != -1) {
			    Vector<String> pages1 = pageNames.remove(index);
			    Vector<String> pages2 = pageNames.remove(index); // We remove index again to get the next element
			    Set<String> result = findIntersection(pages1, pages2);
			    if (result == null) {
			    	pageNames.add(index, null);
			    }
			    else {
				    pageNames.add(index, new Vector<>(result));
			    }
			    operatorList.remove(index); // Remove the used operator
			}

			// Loop for OR operation
			while ((index = operatorList.indexOf("OR")) != -1) {
			    Vector<String> pages1 = pageNames.remove(index);
			    Vector<String> pages2 = pageNames.remove(index); // We remove index again to get the next element
			    Set<String> result = findUnion(pages1, pages2);
			    if (result == null) {
			    	pageNames.add(index, null);
			    }
			    else {
				    pageNames.add(index, new Vector<>(result));
			    }
			    operatorList.remove(index); // Remove the used operator
			}

			// Loop for XOR operation
			while ((index = operatorList.indexOf("XOR")) != -1) {
			    Vector<String> pages1 = pageNames.remove(index);
			    Vector<String> pages2 = pageNames.remove(index); // We remove index again to get the next element
			    Set<String> result = findUnion(pages1, pages2); 
			    if (result == null) {
			    	pageNames.add(index, null);
			    }
			    else {
				    pageNames.add(index, new Vector<>(result));
			    }
			    operatorList.remove(index); // Remove the used operator
			}
			
			// If the pageNames was never populated or it ends up with null
			if (pageNames.size() == 0 || pageNames.get(0) == null) {
				pageNames.add(0,table.getAllPageNames());
			}
			
			Vector<String> pages = pageNames.get(0);
			return filterTuples(pages, arrSQLTerms, strarrOperators).iterator();
		}
	}
	
    private Set<String> findIntersection(Vector<String> column1, Vector<String> column2) {
        if (column1 == null && column2 == null) {
            return null; // Both inputs are null, return null indicating all pages
        } else if (column1 == null) {
            return new HashSet<>(column2); // Return column2's pageList
        } else if (column2 == null) {
            return new HashSet<>(column1); // Return column1's pageList
        } else {
            // Both inputs are not null, perform intersection
            Set<String> resultSet = new HashSet<>(column1);
            resultSet.retainAll(column2);
            return resultSet;
        }
    }
	
	private Set<String> findUnion(Vector<String> column1, Vector<String> column2){
        if (column1 == null || column2 == null) {
            return null; // If any of the inputs are null, return null indicating all pages
        } else {
			Set<String> resultSet = new HashSet<String>(column1);
			resultSet.addAll(column2);
			return resultSet;
        }
	}
	
    public ArrayList<Tuple> filterTuples(Vector<String> pageNames, SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
        ArrayList<Tuple> result = new ArrayList<>();

        // Iterate over each page name
        for (String pageName : pageNames) {
            try {
            	System.out.println(pageName);
                // Load the page from memory
                Page page = FileAccess.readPage(pageName);

                // Iterate over each tuple in the page
                for (Tuple tuple : page.getTupleList()) {
                    // Check if the tuple satisfies the conditions specified by the SQL terms
                    if (tupleSatisfiesConditions(tuple, arrSQLTerms, strarrOperators)) {
                        // If the tuple satisfies the conditions, add it to the result list
                        result.add(tuple);
                    }
                }
            } catch (IOException | ClassNotFoundException e) {
                throw new DBAppException("Error loading page " + pageName + ": " + e.getMessage());
            }
        }

        return result;
    }
    
    private boolean tupleSatisfiesConditions(Tuple tuple, SQLTerm[] arrSQLTerms, String[] strarrOperators) {
        // Evaluate each SQL term and construct a boolean array from the results
        boolean[] termResults = new boolean[arrSQLTerms.length];
        for (int i = 0; i < arrSQLTerms.length; i++) {
            termResults[i] = evaluateTerm(tuple, arrSQLTerms[i]);
        }

        // Apply AND operations
        int index;
        while ((index = Arrays.asList(strarrOperators).indexOf("AND")) != -1) {
            termResults[index + 1] = termResults[index] && termResults[index + 1];
            termResults = removeElement(termResults, index); // Remove the previous term result
            strarrOperators = removeElement(strarrOperators, index); // Remove the used operator
        }

        // Apply OR operations
        while ((index = Arrays.asList(strarrOperators).indexOf("OR")) != -1) {
            termResults[index + 1] = termResults[index] || termResults[index + 1];
            termResults = removeElement(termResults, index); // Remove the previous term result
            strarrOperators = removeElement(strarrOperators, index); // Remove the used operator
        }

        // Apply XOR operations
        while ((index = Arrays.asList(strarrOperators).indexOf("XOR")) != -1) {
            termResults[index + 1] = termResults[index] ^ termResults[index + 1];
            termResults = removeElement(termResults, index); // Remove the previous term result
            strarrOperators = removeElement(strarrOperators, index); // Remove the used operator
        }

        // The last element of termResults will contain the final result
        return termResults[termResults.length - 1];
    }

    // Helper method to remove an element from a boolean array
    private boolean[] removeElement(boolean[] array, int index) {
        boolean[] newArray = new boolean[array.length - 1];
        System.arraycopy(array, 0, newArray, 0, index);
        System.arraycopy(array, index + 1, newArray, index, newArray.length - index);
        return newArray;
    }

    // Helper method to remove an element from a String array
    private String[] removeElement(String[] array, int index) {
        String[] newArray = new String[array.length - 1];
        System.arraycopy(array, 0, newArray, 0, index);
        System.arraycopy(array, index + 1, newArray, index, newArray.length - index);
        return newArray;
    }

    private boolean evaluateTerm(Tuple tuple, SQLTerm term) {
        // Retrieve the column value from the tuple
        Object columnValue = tuple.getColumnValue(term.getColName());

        // Perform the comparison based on the operator
        switch (term.getOperator()) {
            case "=":
                return columnValue.equals(term.getObjValue());
            case ">":
                return ((Comparable) columnValue).compareTo(term.getObjValue()) > 0;
            case ">=":
                return ((Comparable) columnValue).compareTo(term.getObjValue()) >= 0;
            case "<":
                return ((Comparable) columnValue).compareTo(term.getObjValue()) < 0;
            case "<=":
                return ((Comparable) columnValue).compareTo(term.getObjValue()) <= 0;
            case "!=":
                return !columnValue.equals(term.getObjValue());
            default:
                // Unsupported operator
                return false;
        }
    }

    public static void main(String[] args) {

		try {
			/* Ensure that the files in the data folder are deleted before running
			 * Files to delete for 1st run from data folder: tables, indices, pages, metadata file
			 * Some test cases and print statements are included for testing purposes
			 * Page size was reset to 200
			 */ 
			
			String strTableName = "Student";
			String indexName = "studentGPAIndex";
			DBApp dbApp = new DBApp();

			Hashtable htblColNameType = new Hashtable();
			htblColNameType.put("id", "java.lang.Integer");
			htblColNameType.put("name", "java.lang.String");
			htblColNameType.put("gpa", "java.lang.Double");
			
			dbApp.createTable(strTableName, "id", htblColNameType);
			
			dbApp.createIndex(strTableName, "gpa", indexName);

			
			Hashtable htblColNameValue = new Hashtable();
			htblColNameValue.put("id", 3);
			htblColNameValue.put("name", new String("Ahmed Noor"));
			htblColNameValue.put("gpa", 0.95);
			dbApp.insertIntoTable(strTableName, htblColNameValue);

			htblColNameValue.clear();
			htblColNameValue.put("id", 2);
			htblColNameValue.put("name", new String("Ahmed Noor"));
			htblColNameValue.put("gpa", (Double) 0.95);
			dbApp.insertIntoTable(strTableName, htblColNameValue);

			htblColNameValue.clear();
			htblColNameValue.put("id", 5);
			htblColNameValue.put("name", new String("Dalia Noor"));
			htblColNameValue.put("gpa", (Double) 1.25);
			dbApp.insertIntoTable(strTableName, htblColNameValue);

			htblColNameValue.clear();
			htblColNameValue.put("id", 4);
			htblColNameValue.put("name", new String("John Noor"));
			htblColNameValue.put("gpa", (Double) 1.5);
			dbApp.insertIntoTable(strTableName, htblColNameValue);
									
			
			htblColNameValue.clear();
			htblColNameValue.put("id", 9);
			htblColNameValue.put("name", new String("Zaky Noor"));
			htblColNameValue.put("gpa", (Double) 1.12);
			dbApp.insertIntoTable(strTableName, htblColNameValue);
			
			htblColNameValue.clear();
			htblColNameValue.put("id", 7);
			htblColNameValue.put("name", new String("Mohamed Noor"));
			htblColNameValue.put("gpa", (Double) 2.3);
			dbApp.insertIntoTable(strTableName, htblColNameValue);
			
			htblColNameValue.clear();
			htblColNameValue.put("id", 1);
			htblColNameValue.put("name", new String("Omar Noor"));
			htblColNameValue.put("gpa", (Double) 1.7);
			dbApp.insertIntoTable(strTableName, htblColNameValue);
			
			htblColNameValue.clear();
			htblColNameValue.put("id", 11);
			htblColNameValue.put("name", new String("Omar Noor"));
			htblColNameValue.put("gpa", (Double) 1.7);
			dbApp.insertIntoTable(strTableName, htblColNameValue);
			
			htblColNameValue.clear();
			htblColNameValue.put("id", 10);
			htblColNameValue.put("name", new String("Omar Noor"));
			htblColNameValue.put("gpa", (Double) 1.7);
			dbApp.insertIntoTable(strTableName, htblColNameValue);
			
			htblColNameValue.clear();
			htblColNameValue.put("id", 12);
			htblColNameValue.put("name", new String("Omar Noor"));
			htblColNameValue.put("gpa", (Double) 1.7);
			dbApp.insertIntoTable(strTableName, htblColNameValue);
			
			htblColNameValue.clear();
			htblColNameValue.put("id", 8);
			htblColNameValue.put("name", new String("Omar Noor"));
			htblColNameValue.put("gpa", (Double) 1.7);
			dbApp.insertIntoTable(strTableName, htblColNameValue);

						
		} catch (DBAppException exp) {
			System.out.println(exp.getMessage());
		}
	}
	
	public static void displayResultSet(Iterator resultSet) {
	    System.out.println("ResultSet:");
	    while (resultSet.hasNext()) {
	        // Assuming record structure: [ID, Name, GPA]
	        System.out.println(resultSet.next());
	    }
	}
}