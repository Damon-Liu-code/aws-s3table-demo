package com.aws.s3table;

import org.apache.spark.sql.SparkSession;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class S3TableApplication {

    private static final String WAREHOUSE_PATH = "s3://my-test-s3-table-bucket-demo";
    private static final String CATALOG_NAME = "s3tablesbucket";
    private static final String NAMESPACE_NAME = "testdb";
    private static final String TABLE_NAME = "map_data";
    private static final String ARN_PATH = "arn:aws:s3tables:us-west-2:034362073038:bucket/my-test-s3-table-bucket-demo/table/03eefab2-2649-48fc-856c-8f07bf342ce2";
    private static final String REGION = "us-west-2";
    public static void main(String[] args) {
        SpringApplication.run(S3TableApplication.class, args);
        SparkSession session = SparkSession.builder()
                .master("local[*]")
                .config("spark.sql.catalog.s3tablesbucket", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.s3tablesbucket.catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog")
                .config("spark.sql.catalog.s3tablesbucket.warehouse", ARN_PATH)
                .config("spark.sql.catalog.s3tablesbucket.client.region", REGION)
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.jars.packages", "software.amazon.s3:s3-tables-catalog-for-iceberg:0.1.0")
                .getOrCreate();

        session.sql("select * from s3tablesbucket.testdb.map_data").show();
    }

}
