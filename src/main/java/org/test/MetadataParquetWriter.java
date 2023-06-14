package org.test;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;

public class MetadataParquetWriter {
    public static class Builder extends ParquetWriter.Builder<ParquetRecord, Builder> {

        public Builder(Path path) {
            super(path);
        }

        protected Builder(OutputFile path) {
            super(path);
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        protected WriteSupport<ParquetRecord> getWriteSupport(Configuration conf) {
            return new ParquetWriteSupport();
        }
    }
}