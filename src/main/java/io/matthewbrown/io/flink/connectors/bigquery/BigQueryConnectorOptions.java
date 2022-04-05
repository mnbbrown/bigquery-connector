package io.matthewbrown.io.flink.connectors.bigquery;

import org.apache.flink.configuration.ConfigOption;
import static org.apache.flink.configuration.ConfigOptions.key;

public class BigQueryConnectorOptions {
    public static final ConfigOption<String> TABLE_NAME =
            key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name of the BigQuery view or table in $dataset.$table form");

    public static final ConfigOption<String> PROJECT_ID =
            key("project-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The id of the Google Cloud Platform project");
}
