package org.test;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.net.URI;
import java.util.UUID;

@Slf4j
public class Main {

    private static byte[] getRandomHllBytes() throws Exception {
        HyperLogLog hyperLogLog = new HyperLogLog(16);
        for (int i = 0; i < 100; i++) {
            hyperLogLog.offer(UUID.randomUUID().toString());
        }

        return hyperLogLog.getBytes();
    }

    private static byte[] getRandomHllHexBytes() throws Exception {
        HyperLogLog hyperLogLog = new HyperLogLog(16);
        for (int i = 0; i < 100; i++) {
            hyperLogLog.offer(UUID.randomUUID().toString());
        }

        return Hex.encodeHexString(hyperLogLog.getBytes()).getBytes();
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        ParquetWriter parquetWriter = new MetadataParquetWriter.Builder(new Path(new URI("/tmp/test.parquet")))
                .withCompressionCodec(CompressionCodecName.GZIP)
                .withConf(conf)
                .withDictionaryEncoding(true)
                .withByteStreamSplitEncoding(true)
                .withBloomFilterEnabled(true)
                .build();

        parquetWriter.write(new ParquetRecord("Attribute1", getRandomHllBytes(), getRandomHllHexBytes()));
        parquetWriter.write(new ParquetRecord("Attribute1", getRandomHllBytes(), getRandomHllHexBytes()));
        parquetWriter.write(new ParquetRecord("Attribute2", getRandomHllBytes(), getRandomHllHexBytes()));
        parquetWriter.write(new ParquetRecord("Attribute2", getRandomHllBytes(), getRandomHllHexBytes()));
        parquetWriter.close();
        log.info("Successfully created parquet file");
    }
}