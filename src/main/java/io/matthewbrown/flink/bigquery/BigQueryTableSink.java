package io.matthewbrown.flink.bigquery;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;

public class BigQueryTableSink implements DynamicTableSink {
    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return null;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return null;
    }

    @Override
    public DynamicTableSink copy() {
        return null;
    }

    @Override
    public String asSummaryString() {
        return null;
    }
}
