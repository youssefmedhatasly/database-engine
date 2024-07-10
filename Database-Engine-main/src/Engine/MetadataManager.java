package Engine;

import java.io.*;
import java.util.*;

public class MetadataManager {
    private static final String METADATA_FILE = "data/metadata.csv";

    // Method to record metadata for a new table
    public static void recordTableMetadata(String tableName, Hashtable<String, String> colNameType, String clusteringKey) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(METADATA_FILE, true));

        // Record metadata for each column
        for (Map.Entry<String, String> entry : colNameType.entrySet()) {
            String columnName = entry.getKey();
            String columnType = entry.getValue();
            boolean isClusteringKey = columnName.equals(clusteringKey);
            writer.write(tableName + "," + columnName + "," + columnType + "," + isClusteringKey + ",null,null");
            writer.newLine();
        }

        writer.close();
    }

    // Method to record metadata for a new index
    public static void recordIndexMetadata(String tableName, String columnName, String indexName) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(METADATA_FILE));
        List<String> lines = new ArrayList<>();
        String line;

        // Read metadata and update the corresponding index
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split(",");
            String table = parts[0];
            String col = parts[1];
            if (table.equals(tableName) && col.equals(columnName)) {
                parts[4] = indexName;
                parts[5] = "B+ Tree";
                line = String.join(",", parts);
            }
            lines.add(line);
        }
        reader.close();

        // Write updated metadata back to file
        BufferedWriter writer = new BufferedWriter(new FileWriter(METADATA_FILE));
        for (String updatedLine : lines) {
            writer.write(updatedLine);
            writer.newLine();
        }
        writer.close();
    }
}

