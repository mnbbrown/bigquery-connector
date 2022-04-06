package io.matthewbrown.flink.bigquery;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.types.logical.RowType;

public class BigQueryTableSource implements ScanTableSource, SupportsLimitPushDown {
    private final BigQueryOptions options;
    private final ResolvedSchema schema;
    private long limit;

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
        BigQueryRowDataInputFormat inputFormat = new BigQueryRowDataInputFormat(
            options,
            (RowType) schema.toPhysicalRowDataType().getLogicalType()
        );
        inputFormat.setProducedType(runtimeProviderContext.createTypeInformation(this.schema.toPhysicalRowDataType()));

        return InputFormatProvider.of(inputFormat);
    }


    @Override
    public DynamicTableSource copy() {
        return new BigQueryTableSource(this.options, this.schema);
    }

    @Override
    public String asSummaryString() {
        return "BigQuery";
    }

    @Override
    public void applyLimit(long limit) {
        this.limit = limit;
    }
}
