package io.matthewbrown.flink.bigquery;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.HashMap;

// https://cloud.google.com/bigquery/docs/reference/storage#avro_schema_details
public final class BigQueryTypeHelpers {
    private static final Logger LOG = LoggerFactory.getLogger(BigQueryTypeHelpers.class);

    @FunctionalInterface
    public interface Converter extends Serializable {
        Object convert(Object input) throws IOException;
    }

    public static Converter createRowConverter(RowType rowType, Schema avroSchema) {
        LOG.debug("createRowConverter - rowType: " + rowType.toString() + " avro: " + avroSchema.toString(true));
        int arity = rowType.getFieldCount();
        HashMap<Integer, Tuple2<Integer, Converter>> converters = new HashMap<>();
        for (Schema.Field field : avroSchema.getFields()) {
            String name = field.name();
            int index = rowType.getFieldIndex(name);
            converters.put(field.pos(), new Tuple2<>(index, createNullableConverter(rowType.getTypeAt(index), field.schema())));
        }

        return avroObject -> {
            IndexedRecord record = (IndexedRecord) avroObject;
            GenericRowData rowData = new GenericRowData(arity);

            for (Schema.Field field : avroSchema.getFields()) {
                Tuple2<Integer, Converter> converter = converters.get(field.pos());
                rowData.setField(converter.f0, converter.f1.convert(record.get(field.pos())));
            }

            LOG.debug("Converted Avro IndexedRecord to RowData: " + rowData);
            return rowData;
        };
    }

    public static Converter createNullableConverter(LogicalType rowType, Schema avroSchema) {
        Converter converter = createConverter(rowType, avroSchema);
        return avroObject -> {
            if (avroObject == null) {
                return null;
            }
            return converter.convert(avroObject);
        };
    }

    public static Converter createConverter(LogicalType type, Schema avroSchema) {
        switch (type.getTypeRoot()) {
            case NULL:
                return i -> null;
            case TINYINT:
                return avroObject -> ((Integer) avroObject).byteValue();
            case SMALLINT:
                return avroObject -> ((Integer) avroObject).shortValue();
            case BOOLEAN: // boolean
            case INTEGER: // int
            case INTERVAL_YEAR_MONTH: // long
            case BIGINT: // long
            case INTERVAL_DAY_TIME: // long
            case FLOAT: // float
            case DOUBLE: // double
                return i -> i;
            case DATE:
                return avroObject -> convertToDate(avroObject, avroSchema);
            case TIME_WITHOUT_TIME_ZONE:
                return BigQueryTypeHelpers::convertToTime;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return avroObject -> convertToTimestamp(avroObject, avroSchema);
            case CHAR:
            case VARCHAR:
                return avroObject -> StringData.fromString(avroObject.toString());
            case ROW:
                return createRowConverter((RowType) type, avroSchema);
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    // custom converters
    private static TimestampData convertToTimestamp(Object object, Schema avroSchema) throws IOException {
        if (resolveUnionLogicalType(avroSchema).equals("timestamp-micros") || !(object instanceof Long)) {
            throw new IOException("attempted to convert non timestamp BigQuery column to timestamp");
        }
        return TimestampData.fromEpochMillis((Long) object / 1000);
    }

    private static int convertToDate(Object object, Schema avroSchema) {
        if (object instanceof Integer) {
            return (Integer) object;
        } else if (object instanceof LocalDate) {
            return (int) ((LocalDate) object).toEpochDay();
        } else {
            JodaConverter jodaConverter = JodaConverter.getInstance();
            if (jodaConverter != null) {
                return (int) jodaConverter.convertDate(object);
            } else {
                throw new IllegalArgumentException(
                        "Unexpected object type for DATE logical type. Received: " + object);
            }
        }
    }

    private static int convertToTime(Object object) {
        final int millis;
        if (object instanceof Integer) {
            millis = (Integer) object;
        } else if (object instanceof LocalTime) {
            millis = ((LocalTime) object).get(ChronoField.MILLI_OF_DAY);
        } else {
            JodaConverter jodaConverter = JodaConverter.getInstance();
            if (jodaConverter != null) {
                millis = jodaConverter.convertTime(object);
            } else {
                throw new IllegalArgumentException(
                        "Unexpected object type for TIME logical type. Received: " + object);
            }
        }
        return millis;
    }

    private static String resolveUnionLogicalType(Schema schema) throws IOException {
        if (!schema.isUnion()) {
            return schema.getLogicalType().getName();
        }

        for (Schema s: schema.getTypes()) {
            if (s.getLogicalType() != null) {
                return s.getLogicalType().toString();
            }
        }
        throw new IOException(schema.getName() + " needs a defined logicalType");
    }

}
