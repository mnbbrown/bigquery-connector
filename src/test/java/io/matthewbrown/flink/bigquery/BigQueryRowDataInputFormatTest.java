package io.matthewbrown.flink.bigquery;


import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.*;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
        BigQueryReadOptions readOptions = new BigQueryReadOptions();
        List<ResolvedExpression> args = new ArrayList<>();

        // equal
        FieldReferenceExpression fieldReferenceExpression =
                new FieldReferenceExpression("amount_gbp", DataTypes.DOUBLE(), 0, 1);
        ValueLiteralExpression valueLiteralExpression = new ValueLiteralExpression(10);
        args.add(fieldReferenceExpression);
        args.add(valueLiteralExpression);
        CallExpression equalExpression = new CallExpression(BuiltInFunctionDefinitions.EQUALS, args, DataTypes.BOOLEAN());

        readOptions.setFilters(List.of(equalExpression));
        System.out.println(readOptions.filters);
//        BigQueryRowDataInputFormat format = new BigQueryRowDataInputFormat(
//                new BigQueryOptions("gc-prd-risk-eb-bueg","exposure_backfill", "f_payment_CR00005DXHRZA7"),
//                readOptions,
//                rowType
//        );
//        try {
//            format.open(new GenericInputSplit(0, 1));
//            RowData rowData = format.nextRecord(new GenericRowData(3));
//            System.out.println(rowData);
//        } finally {
//            format.close();
//        }
    }
}