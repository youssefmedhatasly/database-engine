package Engine;

public class IndexInfo {
    private String indexName;
    private String colName;

    public IndexInfo(String indexName, String colName) {
        this.indexName = indexName;
        this.colName = colName;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getColName() {
        return colName;
    }
}
