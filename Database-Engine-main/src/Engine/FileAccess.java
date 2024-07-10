package Engine;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/* This class is used to access files (read and write)
 * Serialization and de-serialization
 * Contains methods for table and page access
 */

public class FileAccess {
	
	// get the table object from memory using table name
	public static Table readTable(String tableName) throws ClassNotFoundException, IOException {
		FileInputStream fileStream = new FileInputStream("data/Tables/"+ tableName +".class");
		ObjectInputStream objectStream = new ObjectInputStream(fileStream);
		Table t = (Table) objectStream.readObject();
		objectStream.close();
		return t;
	}
	
	// write the table object into memory
	public static void writeTable(Table t) throws IOException {
		FileOutputStream fileStream = new FileOutputStream("data/Tables/"+ t.getTableName() +".class");
		ObjectOutputStream objectStream = new ObjectOutputStream(fileStream);
		objectStream.writeObject(t);
		objectStream.flush();
		objectStream.close();
	}
	
	//Create the table file
    public static void createTableFile(String tableName) throws DBAppException {        
        // Create the file
        File tableFile = new File("data/Tables/" + tableName + ".class");
        try {
            if (tableFile.createNewFile()) {
                System.out.println("Table file created: " + tableFile.getName());
            } else {
                throw new DBAppException("Table file already exists");
            }
        } catch (IOException e) {
            throw new DBAppException("Error creating table file: " + e.getMessage());
        }
    }
	
	// get the page object from the memory using table name and id (page name)
	public static Page readPage(String pageName) throws ClassNotFoundException, IOException {
		FileInputStream fileStream = new FileInputStream("data/Pages/"+ pageName +".class");
		ObjectInputStream objectStream = new ObjectInputStream(fileStream);
		Page p = (Page) objectStream.readObject();
		objectStream.close();
		return p;
	}
	
	// write the page object into memory
	public static void writePage(Page p) throws IOException {
		FileOutputStream fileStream = new FileOutputStream("data/Pages/"+ p.getPageName() +".class");
		ObjectOutputStream objectStream = new ObjectOutputStream(fileStream);
		objectStream.writeObject(p);
		objectStream.flush();
		objectStream.close();
	}
	
	// Create the page file
    public static void createPageFile(String pageName) throws DBAppException {
        // Create the file for the new page
        File pageFile = new File("data/Pages/" + pageName + ".class");
        try {
            if (pageFile.createNewFile()) {
                System.out.println("Page file created: " + pageFile.getName());
            } else {
                throw new DBAppException("Page file already exists");
            }
        } catch (IOException e) {
            throw new DBAppException("Error creating new page file: " + e.getMessage());
        }
    }
    
    public static void deletePageFile(String pageName) throws DBAppException{
    	File pageFile = new File("data/Pages/" + pageName + ".class");
    	if(!pageFile.delete()) {
    		throw new DBAppException("File was not deleted successfully");
    	}
    }
    
    public static Index readIndex(String indexName) throws ClassNotFoundException, IOException {
        FileInputStream fileStream = new FileInputStream("data/Indices/" + indexName + ".class");
        ObjectInputStream objectStream = new ObjectInputStream(fileStream);
        Index index = (Index) objectStream.readObject();
        objectStream.close();
        return index;
    }
    
    public static void writeIndex(Index index) throws IOException {
        FileOutputStream fileStream = new FileOutputStream("data/Indices/" + index.getIndexName() + ".class");
        ObjectOutputStream objectStream = new ObjectOutputStream(fileStream);
        objectStream.writeObject(index);
        objectStream.flush();
        objectStream.close();
    }
    
    public static void createIndexFile(String indexName) throws DBAppException {
        File indexFile = new File("data/Indices/" + indexName + ".class");
        try {
            if (indexFile.createNewFile()) {
                System.out.println("Index file created: " + indexFile.getName());
            } else {
                throw new DBAppException("Index file already exists");
            }
        } catch (IOException e) {
            throw new DBAppException("Error creating index file: " + e.getMessage());
        }
    }
}
