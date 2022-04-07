package io.matthewbrown.flink.bigquery;

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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class BigQueryRowDataInputFormat extends RichInputFormat<RowData, InputSplit> implements ResultTypeQueryable<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryRowDataInputFormat.class);

    private transient boolean hasNext = false;

    private TypeInformation<RowData> typeInformation;
    private final RowType rowType;
    private Iterator<ReadRowsResponse> stream;
    private final BigQueryOptions options;
    private final BigQueryReadOptions readOptions;

    private BinaryDecoder decoder = null;
    private DatumReader<GenericRecord> datumReader = null;
    private BigQueryReadClient client = null;
    private BigQueryTypeHelpers.Converter converter = null;

    public BigQueryRowDataInputFormat(BigQueryOptions options, BigQueryReadOptions readOptions, RowType rowType) {
        this.options = options;
        this.readOptions = readOptions;
        this.rowType = rowType;
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
        LOG.debug("opening split " + inputSplit.getSplitNumber());
        client = BigQueryReadClient.create();

        ReadSession.TableReadOptions.Builder tableReadOptionsBuilder = ReadSession.TableReadOptions.newBuilder();
        // query fields
        tableReadOptionsBuilder.addAllSelectedFields(this.rowType.getFieldNames());
        // apply restrictions
        for (String filter : readOptions.getFilters()) {
            tableReadOptionsBuilder.setRowRestriction(filter);
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
        converter = BigQueryTypeHelpers.createNullableConverter(rowType, schema);

        // start
        hasNext = stream.hasNext();
        LOG.debug("opened split " + inputSplit.getSplitNumber());
    }

    @Override
    public boolean reachedEnd() {
        LOG.debug("reachedEnd: " + !this.hasNext);
        return !this.hasNext;
    }

    @Override
    public RowData nextRecord(RowData reuse) throws IOException {
        LOG.debug("loading next record");
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

        RowData converted = (RowData) this.converter.convert(result);
        LOG.debug("emitting record: " + converted);
        return converted;
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
            LOG.debug("closing client");
            client.close();
        }
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return typeInformation;
    }

    public void setProducedType(TypeInformation<RowData> typeInformation) {
        this.typeInformation = typeInformation;
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
