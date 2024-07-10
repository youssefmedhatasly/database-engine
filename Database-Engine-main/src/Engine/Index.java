package Engine;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Vector;

import bplustree.BPlusTree;
import bplustree.Key;

public class Index implements Serializable{
    private BPlusTree tree;
    private String indexName;

    public Index(String indexName, int order) throws DBAppException {
		FileAccess.createIndexFile(indexName);

        this.setIndexName(indexName);
        tree = new BPlusTree(order);
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public void delete(Object key, String pageName) throws DBAppException {
        tree.delete(new Key(key), pageName);
        // Write the updated index back to memory
        System.out.println(this);
        try {
            FileAccess.writeIndex(this);
        } catch (IOException e) {
            throw new DBAppException("Error writing index to memory: " + e.getMessage());
        }

    }
    
    public void delete(Object key) throws DBAppException {
        tree.delete(new Key(key));
        // Write the updated index back to memory
        System.out.println(this);
        try {
            FileAccess.writeIndex(this);
        } catch (IOException e) {
            throw new DBAppException("Error writing index to memory: " + e.getMessage());
        }
    }

    public void insert(Object key, String pageName) throws DBAppException {
        tree.insert(new Key(key), pageName);
        // Write the updated index back to memory
        System.out.println(this);
        try {
            FileAccess.writeIndex(this);
        } catch (IOException e) {
            throw new DBAppException("Error writing index to memory: " + e.getMessage());
        }
    }

    public void update(Object key, String oldPageName, String newPageName) throws DBAppException {
        tree.update(new Key(key), oldPageName, newPageName);
        // Write the updated index back to memory
        System.out.println(this);
        try {
            FileAccess.writeIndex(this);
        } catch (IOException e) {
            throw new DBAppException("Error writing index to memory: " + e.getMessage());
        }
    }

    public Vector<String> search(Object key) {
		Vector<String> searchResult = tree.search(new Key(key));
		System.out.println(this);
		if(searchResult == null) {
			return new Vector<>();
		}
		HashSet<String> uniqueSet = new HashSet<>(searchResult);
		return new Vector<>(uniqueSet);
    }

    public Vector<String> searchWithBounds(Object bound, String operator) {
        ArrayList<Vector<String>> searchResults = tree.searchWithBounds(new Key(bound), operator);
        System.out.println(this);
        HashSet<String> uniquePages = new HashSet<>();
        System.out.println(searchResults);
        // Flatten the search results and add them to the set
        for (Vector<String> result : searchResults) {
            uniquePages.addAll(result);
        }

        // Convert the set to a vector
        return new Vector<>(uniquePages);
    }


    @Override
    public String toString() {
        return tree.toString();
    }
}
