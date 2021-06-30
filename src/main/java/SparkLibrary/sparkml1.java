package SparkLibrary;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class sparkml1 {
    public static void main(String[] args) {

        //////////////////////////////////// Reading Data ////////////////////////////////////////
        //create logger
        Logger.getLogger("org").setLevel(Level.ERROR);

        //create spark session
        SparkSession sparkSession = SparkSession.builder().appName("ml1").master("local[*]").getOrCreate();

        //create DataFrame reader
        DataFrameReader dataFrameReader = sparkSession.read();
        dataFrameReader.option("header","true");

        //load dataset
        Dataset<Row> data = dataFrameReader.csv("src/main/resources/listings.csv");
        //print schema to show column names and data types
        data.printSchema();

        //////////////////////////////////////// Modifying and cleaning Data/////////////////////////////////////////
        //create temp view to to use sql
        data.createOrReplaceTempView("DATA");

        //selecting specific columns and casting its type
        Dataset<Row> modifiedData = sparkSession.sql("SELECT cast(id as int) id,cast(bedrooms as float) bedrooms, " +
                "cast(minimum_nights as float) minimum_nights, " +
                "cast(number_of_reviews as float) number_of_reviews, cast(price as float)price FROM DATA");

        //check selection
        modifiedData.show();
        modifiedData.printSchema();

        //remove nulls
        System.out.println("Before removing NA, there are " + modifiedData.count() + " record");
        modifiedData = modifiedData.na().drop();
        System.out.println("After removing NA, there are " + modifiedData.count() + " record");

        //removing duplicates
        System.out.println("The number of nun duplicated records " + modifiedData.distinct().count() + " record");
        modifiedData = modifiedData.dropDuplicates();
        System.out.println("After removing duplicates total number of records " + modifiedData.count() + " record");


        //////////////////////////////  Preparing Data For Machine learning /////////////////////////////////////
        // Combine multiple input columns to a Vector using Vector Assembler  That will contain the features
        VectorAssembler vectorAssembler = new VectorAssembler();
        String[] inputColumns = {"bedrooms","minimum_nights","number_of_reviews"};
        vectorAssembler.setInputCols(inputColumns);
        vectorAssembler.setOutputCol("features");

        // transform data using vector assembler
        Dataset<Row> modifiedDataTransformed = vectorAssembler.transform(modifiedData);
        //check
        modifiedDataTransformed.show(2);

        // divide data to train and test data 80/20 and set seed to 42
        double[] split = {0.8,0.2};
        var cleanedData = modifiedDataTransformed.randomSplit(split,42);

        var trainData = cleanedData[0];
        var testData = cleanedData[1];
        //check
        System.out.println("Train Data " +trainData.count() + " record");
        System.out.println("Test Data " +testData.count() + " record");

        //////////////////////////////   Machine learning (linear regression) /////////////////////////////////////
        //Create a LinearRegression Estimator and set the feature column and the label column
        LinearRegression linearRegression = new LinearRegression();
        linearRegression.setFeaturesCol("features");
        linearRegression.setLabelCol("price");

        // create linear regression model
        LinearRegressionModel Model = linearRegression.fit(trainData);

        //getting the coefficient and the intercept
        var coef = Model.coefficients().toArray()[0];
        var intercept = Model.intercept();

        System.out.println("The formula for the linear regression line is price = "+coef+"*bedrooms + "+intercept);

        //predictions
        var predictions= Model.transform(testData);
        predictions.show();







    }
}
