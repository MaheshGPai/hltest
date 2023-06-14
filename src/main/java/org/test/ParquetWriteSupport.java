package org.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.util.HashMap;
import java.util.Map;

public class ParquetWriteSupport extends WriteSupport<ParquetRecord> {
    private static final String PARQUET_SCHEMA = """
            message Data {
               required binary AttributeName (STRING);
               required binary HLLBytes;
               required binary HLLHexBytes;
             }""";
    private MessageType schema;
    private RecordConsumer recordConsumer;
    private Map<String, Integer> cols = new HashMap<>();

    public ParquetWriteSupport() {
        schema = MessageTypeParser.parseMessageType(PARQUET_SCHEMA);
        for (int i = 0; i < schema.getColumns().size(); i++) {
            cols.put(schema.getColumns().get(i).getPath()[0], i);
        }
    }

    @Override
    public WriteContext init(Configuration config) {
        return new WriteContext(schema, new HashMap());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

    @Override
    public void write(ParquetRecord record) {
        recordConsumer.startMessage();

        recordConsumer.startField("AttributeName", 0);
        recordConsumer.addBinary(Binary.fromString(record.getAttributeName()));
        recordConsumer.endField("AssetAlias", 0);

        recordConsumer.startField("HLLBytes", 1);
        recordConsumer.addBinary(Binary.fromConstantByteArray(record.getHllBytes()));
        recordConsumer.endField("HLLBytes", 1);

        recordConsumer.startField("HLLHexBytes", 2);
        recordConsumer.addBinary(Binary.fromConstantByteArray(record.getHllHexBytes()));
        recordConsumer.endField("HLLHexBytes", 2);

        recordConsumer.endMessage();
    }
}
