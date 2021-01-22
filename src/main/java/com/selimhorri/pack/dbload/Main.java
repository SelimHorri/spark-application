package com.selimhorri.pack.dbload;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import java.util.Properties;

public class Main {
	
	private static SparkSession sparkSession;
	
	public static void main(String[] args) {
		
		sparkSession = Main.createSparkSession("CSV to DB", "local");
		Dataset<Row> df = Main.createDataframeWithHeader("csv", "src/main/resources/name_job.txt");
		
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
	
	public static SparkSession createSparkSession(final String appName, final String master) {
		return new SparkSession.Builder().appName(appName).master(master).getOrCreate();
	}
	
	public static Dataset<Row> createDataframeWithHeader(final String fileFormat, final String filePath) {
		return sparkSession.read().format(fileFormat).option("header", true).load(filePath);
	}
	
	
	
}










