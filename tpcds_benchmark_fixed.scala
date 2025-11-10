import com.databricks.spark.sql.perf.tpcds.TPCDS
import com.databricks.spark.sql.perf.tpcds.TPCDSTables

val databaseName = "tpcds_sf1000"  // Or whatever scale factor you used

// Configuration
val baseLocation = s"s3://<>/tables" // replace with your data location 
val scaleFactor = "1000"
val dataLocation = s"$baseLocation/tpcds_$scaleFactor"  // Path to your existing TPCDS data
val resultLocation = s"$baseLocation/results"  // Where to save results

// Create TPCDS tables instance
// Note: dsdgenDir is required but not used when only creating external tables
// You can pass an empty string or any dummy value since you're not generating data
val tables = new TPCDSTables(
  sqlContext = spark.sqlContext,
  dsdgenDir = "",  // Not needed since data already exists
  scaleFactor = scaleFactor
)

// Create external tables pointing to your existing data
// Note: createExternalTables already creates the database and switches to it
tables.createExternalTables(
  location = dataLocation,           // Your existing data location
  format = "parquet",                // Format (assuming parquet)
  databaseName = databaseName,       // Database name
  overwrite = false,                 // Overwrite if tables exist
  discoverPartitions = true          // Auto-discover partitions
)

// Switch to the TPCDS database (already done by createExternalTables, but safe to do again)
sql(s"use $databaseName")

val iterations = 1  // Number of iterations to run each query
val timeout = 24 * 60 * 60  // 24 hours timeout in seconds

// Create TPCDS benchmark instance
val tpcds = new TPCDS(sqlContext = spark.sqlContext)

// Get queries - you can filter as needed
val queries = tpcds.tpcds2_4Queries.take(50)  // Get first 50 queries
// Or use all queries:
// val queries = tpcds.tpcds2_4Queries
// Or filter by name:
// val queries = tpcds.tpcds2_4Queries.filter(q => q.name == "q57-v2.4")

// Run experiment with all queries
// Note: runExperiment takes Seq[Benchmarkable], and Query extends Benchmarkable
// Using .checkResult converts queries to HashResults mode for result validation
val experiment = tpcds.runExperiment(
  executionsToRun = queries.map(_.checkResult),  // All selected queries with result checking
  iterations = iterations,
  resultLocation = resultLocation,
  tags = Map(
    "runtype" -> "benchmark",
    "database" -> databaseName,
    "scale_factor" -> scaleFactor,
    "spark_version" -> "4.0+"
  )
)

println("Experiment started!")
println(s"Experiment ID: ${experiment.timestamp}")
println(s"Results will be saved to: ${experiment.resultPath}")

// Monitor progress
println("Experiment started! Monitoring progress...")
// waitForFinish takes timeout in seconds (Int)
experiment.waitForFinish(timeout.toInt)

