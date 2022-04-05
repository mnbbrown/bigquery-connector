package io.matthewbrown.io.flink.connectors.bigquery;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;

public class BigQueryTableSource implements ScanTableSource {
    private final BigQueryOptions options;
    private final ResolvedSchema schema;

    public BigQueryTableSource(BigQueryOptions options, ResolvedSchema schema) {
        this.options = options;
        this.schema = schema;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        return InputFormatProvider.of(
                new BigQueryRowDataInputFormat(
                        options,
                        schema,
                        runtimeProviderContext.createTypeInformation(this.schema.toPhysicalRowDataType())
                )
        );
    }


    @Override
    public DynamicTableSource copy() {
        return new BigQueryTableSource(this.options, this.schema);
    }

    @Override
    public String asSummaryString() {
        return "BigQuery";
    }
}
