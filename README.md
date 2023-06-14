# Parquet Schema

```
message Data {
    required binary AttributeName (STRING);
    required binary HLLBytes;
    required binary HLLHexBytes;
}
```

# Athena Table

```sql
CREATE EXTERNAL TABLE HLLTestTable(
  attributename string,
  hllBytes binary,
  hllHexBytes string)
STORED AS PARQUET
LOCATION 's3://<S3BucketName>'
```

# Execution Step

1. mvn clean install
2. rm -f `/tmp/test.parquet` (If file exists)
3. `java -jar target/hlltest-1.0-SNAPSHOT-jar-with-dependencies.jar`
4. Copy `/tmp/test.parquet` to the appropriate S3 bucket.