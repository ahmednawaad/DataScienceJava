package SparkLibrary;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;



public class SparkSQL1 {
    public static void main(String[] args) {
        //making logger
        Logger.getLogger("org").setLevel(Level.ERROR);

        // create spark session
        SparkSession sparkSession = SparkSession.builder().appName("sql1").master("local[*]").getOrCreate();

        //create dataframe reader
        DataFrameReader dataFrameReader = sparkSession.read();
        dataFrameReader.option("header","true");

        //read data as dataset
        Dataset<Row> csvDataFrame = dataFrameReader.csv ("src/main/resources/data_2.csv");
        csvDataFrame.printSchema();

        //// Create view and execute query to convert types as, by default, all columns have string types
        csvDataFrame.createOrReplaceTempView("tempView1");

//        Dataset<Row> tempView1Data = sparkSession.sql(
//                "SELECT CAST(id as int) id, CAST(date as string) date, CAST(Temperature as float) Temperature, "+
//                "CAST(Humidity as float) Humidity, CAST(Light as float) Light, CAST(CO2 as float) CO2, "+
//                "CAST(HumidityRatio as float) HumidityRatio, CAST(Occupancy as int) Occupancy FROM tempView1");
//
//        tempView1Data.summary();
//
//        tempView1Data.printSchema();
//
//        // Create view to execute query to get filtered data
//        tempView1Data.createOrReplaceTempView ("ROOM_OCCUPANCY");sparkSession.sql (
//                "SELECT * FROM ROOM_OCCUPANCY WHERE Temperature >= 23.6 AND Humidity > 27 AND Light > 500 "+
//                        "AND CO2 BETWEEN 920 and 950").show ();





    }
}
