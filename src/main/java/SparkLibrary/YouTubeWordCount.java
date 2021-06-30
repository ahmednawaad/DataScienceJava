package SparkLibrary;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.Collectors;

public class YouTubeWordCount {
    public static void main(String[] args) {
        String filePath = "src/main/resources/USvideos.csv";
        Logger.getLogger("org").setLevel(Level.ERROR);

        //spark configuration and set number cores
        SparkConf conf = new SparkConf().setAppName("CountWords").setMaster("local[4]");

        //spark context
        JavaSparkContext sparkContext =  new JavaSparkContext(conf);

        //loading data
        JavaRDD<String> videos = sparkContext.textFile(filePath);

        //Make a transformation
        JavaRDD<String> titles = videos.map(line -> {try {return line.split(",")[2];} catch (ArrayIndexOutOfBoundsException e) {return "";}});

        JavaRDD<String> words = titles.flatMap(title -> Arrays.
                asList(title.toLowerCase().trim().replaceAll ("\\p{Punct}", "").split(" ")).iterator());

        ///counting

        Map<String,Long> wordCounts = words.countByValue();

        // sorting
        List<Map.Entry> sorted = wordCounts.entrySet().stream().sorted(Map.Entry.comparingByValue()).collect(Collectors.toList());

        //printing sorted list
        sorted.forEach( entry -> System.out.println(entry.getKey() +" : "+ entry.getValue()));


    }
}
