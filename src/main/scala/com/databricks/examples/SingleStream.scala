/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package com.databricks.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

/** Computes an approximation to pi */
object SingleStream {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Single Stream")
      .getOrCreate()
    import spark.implicits._

    val jsonSchema = new StructType().add("change", DoubleType).add("price", DoubleType).add("sector", StringType).add("ticker_symbol", StringType)
    
    val inputPath = "/mnt/databricks-data/kinesis/2017/03/10/20/*"
    
    println("Starting structured streaming app ...")
    val streamReader = spark.readStream
      .schema(jsonSchema)
      .option("maxFilesPerTrigger", 2)
      .json(inputPath)
    
    streamReader.writeStream
        .option("checkpointLocation", "dbfs:/mnt/databricks-data/ss-test-checkpoint")
        .outputMode("append")
        .trigger(Trigger.Once).format("parquet").start("dbfs:/mnt/databricks-data/ss-test-jar/")
    
    // COMMAND ----------
    
    val df = spark.read.parquet("/mnt/databricks-data/ss-test-jar")
    df.show()
  }
}
// scalastyle:on println
