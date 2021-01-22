package com.selimhorri.pack.ex2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class InferCSVSchema {
	
	public void printSchema() {
		
		final SparkSession sparkSession = new SparkSession.Builder().appName("Complex CSV to Dataframe").master("local").getOrCreate();
		
		Dataset<Row> df = sparkSession.read().format("csv").option("header", true).option("multiline", true).option("sep", ";").option("quote", "^").option("dateFormat", "M/d/y").option("inferSchema", true).load("src/main/resources/amazon_products.txt");
		
		System.out.println("Expect of the dataframe content");
		df.show(7);
		df.show(7, 150);
		System.out.println("Dataframe's Schema");
		df.printSchema();
		
	}
	
}









