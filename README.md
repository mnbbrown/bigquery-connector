
#### Usage

Configure maven to use the Github package registry: https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-apache-maven-registry

```xml
  <dependency>
    <groupId>io.matthewbrown.flink</groupId>
    <artifactId>bigquery-connector</artifactId>
    <version>1.0-SNAPSHOT</version>
  </dependency>
```

```java
tableEnv.createTemporaryTable("", TableDescriptor.forConnector("bigquery")
  .schema(Schema.newBuilder()
          .column("id", DataTypes.STRING())
          .column("name", DataTypes.STRING())
          .column("created_at", DataTypes.TIMESTAMP(3))
          .column("amount", DataTypes.DOUBLE())
          .watermark("created_at", "created_at - INTERVAL '5' SECOND")
          .build())
  .option(BigQueryConnectorOptions.PROJECT_ID, "")
  .option(BigQueryConnectorOptions.TABLE_NAME, "$dataset.$table")
  .build());
```
