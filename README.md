Custom (URI Pattern Based) AWS credentials provider

See: https://aws.amazon.com/blogs/big-data/securely-analyze-data-from-another-aws-account-with-emrfs/

Set configs before accessing S3:

```scala
spark.sparkContext.hadoopConfiguration.set("awscp-role-arn", "arn:aws:iam::111222333444:data_analyst")
spark.sparkContext.hadoopConfiguration.set("awscp-uri-pattern", "^s3://something/.+parquet-std")
```

```python
sc._jsc.hadoopConfiguration().set("awscp-role-arn", "arn:aws:iam::111222333444:data_analyst")
sc._jsc.hadoopConfiguration().set("awscp-uri-pattern", "^s3://something/.+parquet-std")
```
