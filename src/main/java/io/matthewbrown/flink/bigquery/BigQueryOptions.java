package io.matthewbrown.flink.bigquery;

import java.io.Serializable;

public class BigQueryOptions implements Serializable {
    private String projectId;
    private String datasetName;
    private String tableName;

    BigQueryOptions() {}
    BigQueryOptions(String projectId, String datasetName, String tableName) {
        this.projectId = projectId;
        this.tableName = tableName;
        this.datasetName = datasetName;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public void setDatasetName(String datasetName) {
        this.datasetName = datasetName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getFullyQualifiedTableName() {
        return "projects/" + getProjectId() + "/datasets/" + getDatasetName() + "/tables/" + getTableName();
    }

    public String getProjectId() {
        return projectId;
    }
}

