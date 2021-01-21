package com.selimhorri.pack;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import java.util.Properties;

public class Main {
	
	public static void main(String[] args) {
		
		final SparkSession sparkSession = new SparkSession.Builder().appName("CSV to DB").master("local").getOrCreate();
		
		Dataset<Row> df = sparkSession.read().format("csv").option("header", true).load("src/main/resources/name_job.txt");
		
		df = df.withColumn("fullName", concat(lit("FIRST_NAME => "), df.col("first_name"), lit(" || "), lit("LAST_NAME => "), df.col("last_name")) );
		df.show();
		
		final String dbUrl = "jdbc:mysql://localhost:3306/spark_db";
		final Properties properties = new Properties();
		// properties.setProperty("driver", "");
		properties.setProperty("user", "root");
		properties.setProperty("password", "");
		
		df.write().mode(SaveMode.Overwrite).jdbc(dbUrl, "persons", properties);
		System.out.println("====>> Loaded in the Database <<====");
		
	}
	
	
	
}










