package io.matthewbrown.io.flink.connectors.bigquery;

import com.google.cloud.bigquery.storage.v1.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class BigQueryRowDataInputFormat extends RichInputFormat<RowData, InputSplit> implements ResultTypeQueryable<RowData> {

    private transient boolean hasNext = false;
    private final TypeInformation<RowData> typeInformation;
    private Iterator<ReadRowsResponse> stream;
    private final List<String> columns;
    private final BigQueryOptions options;

    private BinaryDecoder decoder = null;
    private DatumReader<GenericRecord> datumReader = null;
    private BigQueryReadClient client = null;

    public BigQueryRowDataInputFormat(BigQueryOptions options, ResolvedSchema schema, TypeInformation<RowData> typeInformation) {
        this.options = options;
        this.columns = schema.getColumns().stream().map(Column::getName).collect(Collectors.toList());
        this.typeInformation = typeInformation;
    }

    @Override
    public void configure(Configuration configuration) {
        // no-op
    }


    @Override
    public void openInputFormat() {
        // no-op
    }

    @Override
    public void closeInputFormat() {
        // no-op
    }

    @Override
    public void open(InputSplit inputSplit) throws IOException {
        client = BigQueryReadClient.create();
        ReadSession.TableReadOptions.Builder tableReadOptionsBuilder = ReadSession.TableReadOptions.newBuilder();
        for (String c : columns) {
            tableReadOptionsBuilder.addSelectedFields(c);
        }

        ReadSession.Builder sessionBuilder =
                ReadSession.newBuilder()
                        .setTable(options.getFullyQualifiedTableName())
                        .setDataFormat(DataFormat.AVRO)
                        .setReadOptions(tableReadOptionsBuilder.build());

        CreateReadSessionRequest.Builder builder =
                CreateReadSessionRequest.newBuilder()
                        .setParent(String.format("projects/%s", options.getProjectId()))
                        .setReadSession(sessionBuilder)
                        .setMaxStreamCount(1);

        // connect
        ReadSession session = client.createReadSession(builder.build());
        String streamName = session.getStreams(0).getName();
        ReadRowsRequest readRowsRequest = ReadRowsRequest.newBuilder().setReadStream(streamName).build();
        stream = client.readRowsCallable().call(readRowsRequest).iterator();

        // build schema
        String avroSchema = session.getAvroSchema().getSchema();
        Schema schema = new Schema.Parser().parse(avroSchema);
        datumReader = new GenericDatumReader<>(schema);

        // start
        hasNext = stream.hasNext();
    }

    @Override
    public boolean reachedEnd() {
        return !this.hasNext;
    }

    @Override
    public RowData nextRecord(RowData reuse) throws IOException {
        GenericRecord result;
        if ( decoder != null && ! decoder.isEnd() ) {
            result = datumReader.read(null, decoder);
        } else {
            fetchNextAvroRows();

            if ( decoder != null && ! decoder.isEnd() ) {
                result = datumReader.read(null, decoder);
            } else {
                result = null; // end of traversal
                hasNext = false;
            }
        }
        if (result == null) {
            return null;
        }

        List<Schema.Field> fields = result.getSchema().getFields();
        GenericRowData rowData = new GenericRowData(RowKind.INSERT, result.getSchema().getFields().size());

        for (Schema.Field f: fields) {
            int pos = this.columns.indexOf(f.name());
            rowData.setField(pos, result.get(f.name()));
        }
        return rowData;
    }

    private void fetchNextAvroRows() {
        if ( stream.hasNext() ) {
            AvroRows currentAvroRows = stream.next().getAvroRows();
            decoder = DecoderFactory.get()
                    .binaryDecoder(currentAvroRows.getSerializedBinaryRows().toByteArray(), decoder);
        } else {
            decoder = null;
        }
    }

    @Override
    public void close() {
        if (client != null && !client.isShutdown()) {
            client.close();
        }
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return typeInformation;
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics baseStatistics) {
        return baseStatistics;
    }

    @Override
    public InputSplit[] createInputSplits(int i) {
        return new GenericInputSplit[]{new GenericInputSplit(0, 1)};
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

}
