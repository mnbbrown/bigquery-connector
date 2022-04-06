package io.matthewbrown.flink.bigquery;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

public class BigQueryTableSourceFactory implements DynamicTableSourceFactory {
    public static final String IDENTIFIER = "bigquery";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();
        helper.validate();
        ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
        return new BigQueryTableSource(getBigQueryOptions(config), schema);
    }

    private BigQueryOptions getBigQueryOptions(ReadableConfig readableConfig) {
        BigQueryOptions options = new BigQueryOptions();
        options.setTableName(readableConfig.get(BigQueryConnectorOptions.TABLE_NAME));
        readableConfig.getOptional(BigQueryConnectorOptions.PROJECT_ID).ifPresent(options::setProjectId);
        return options;
    }


    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(BigQueryConnectorOptions.TABLE_NAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(BigQueryConnectorOptions.PROJECT_ID);
        return options;
    }
}
