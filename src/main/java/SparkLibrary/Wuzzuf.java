package SparkLibrary;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class Wuzzuf {
    public static void main(String[] args) {
        //////////////////////////////////// Reading Data ////////////////////////////////////////
        //create logger
        Logger.getLogger("org").setLevel(Level.ERROR);

        //create spark session
        SparkSession sparkSession = SparkSession.builder().appName("ml1").master("local[*]").getOrCreate();

        //create DataFrame reader
        DataFrameReader dataFrameReader = sparkSession.read();
        dataFrameReader.option("header","true");
        dataFrameReader.option("sep","%");

        //load dataset
        Dataset<Row> dataset = dataFrameReader.csv("src/main/resources/Wuzzuf_Jobs_delimiter.csv");
        //print schema to show column names and data types

        dataset.printSchema();

        var features = dataset.select("Type").collect().toString();

//        var encodedFeatures = features.flatMap(name -> {
//
//            var stringIndexer = new StringIndexer()
//                    .setInputCol("Type")
//                    .setOutputCol(name+"_Index");
//
//            var oneHotEncoder = new OneHotEncoder()
//                    .setInputCol(name+ "_Index")
//                    .setOutputCol(name+"_vec")
//                    .setDropLast(true);
//
//
//        });

//        var indexer = new StringIndexer()
//                .setInputCol("Type")
//                .setOutputCol("TypeIndex")
//                .fit(dataset);
//
//        Dataset<Row> indexedDataset = indexer.transform(dataset);
//
////        var encoder = new OneHotEncoder()
////                .setInputCol("TypeIndex")
////                .setOutputCol("TypeVec");
////
////        Dataset<Row> encoded = encoder.transform(indexed);

        var indexedDataset = encodeCategoricalFeatures(dataset, List.of("Type","Level","YearsExp"));
                indexedDataset.show();


    }
    public static Dataset<Row> encodeCategoricalFeatures(Dataset<Row> dataset, List<String> features ){

        for (var feature: features){
            var indexer = new StringIndexer()
                    .setInputCol(feature)
                    .setOutputCol(feature+"_indexed")
                    .fit(dataset);
            dataset = indexer.transform(dataset);

            var encoder = new OneHotEncoder()
                    .setInputCol(feature+"_indexed")
                    .setOutputCol(feature+"_vec")
                    .setDropLast(true);

            dataset = encoder.transform(dataset);
        }
        return dataset;

    }

}
