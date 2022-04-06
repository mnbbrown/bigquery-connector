package io.matthewbrown.flink.bigquery;


import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

class BigQueryRowDataInputFormatTest {

    private static final String[] fieldNames = new String[] {"organisation_id", "amount_gbp", "created_at"};
    private static final DataType[] fieldDataTypes =
            new DataType[] {
                    DataTypes.STRING(),
                    DataTypes.DOUBLE(),
                    DataTypes.TIMESTAMP(3)
            };

    final RowType rowType =
            RowType.of(
                    Arrays.stream(fieldDataTypes)
                            .map(DataType::getLogicalType)
                            .toArray(LogicalType[]::new),
                    fieldNames);

    @Test
    void SimpleTest() throws IOException {
        BigQueryRowDataInputFormat format = new BigQueryRowDataInputFormat(
                new BigQueryOptions("bigquery-public-data","samples.gsod"),
                rowType
        );
        try {
            format.open(new GenericInputSplit(0, 1));
            RowData rowData = format.nextRecord(new GenericRowData(3));
            System.out.println(rowData);
        } finally {
            format.close();
        }
    }
}