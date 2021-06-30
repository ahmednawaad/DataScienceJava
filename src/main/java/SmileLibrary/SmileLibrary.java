package SmileLibrary;

import smile.data.DataFrame;
import smile.data.formula.Formula;
import smile.data.measure.NominalScale;
import smile.data.vector.IntVector;
import smile.plot.swing.Histogram;
import smile.plot.swing.ScatterPlot;
import smile.regression.RandomForest;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.Arrays;

import static java.awt.Color.red;

public class SmileLibrary {


    public static void main(String[] args) throws IOException, URISyntaxException, InvocationTargetException, InterruptedException {

        //read passenger data
        String trainDataPath = "src/main/resources/titanic_train.csv";
        String TestDataPath = "src/main/resources/titanic_test.csv";
        PassengerProvider passengerProvider = new PassengerProvider();
        DataFrame trainDF = passengerProvider.readCSVFile(trainDataPath);

        DataFrame testDF = passengerProvider.readCSVTestFile(TestDataPath);

        //EDA
        //First Print Summary
        System.out.println(trainDF.summary());

        //Data Cleaning
        //Convert Sex to numeric Values
        trainDF = trainDF.merge(IntVector.of("Gender", encodeCategory (trainDF, "Sex")));
        trainDF = trainDF.merge(IntVector.of("PClass", encodeCategory (trainDF, "Pclass")));


        //dropping unneeded columns
        trainDF = trainDF.drop("Name");
        trainDF = trainDF.drop("Sex");
        trainDF = trainDF.drop("Pclass");


        //delete nulls
        trainDF = trainDF.omitNullRows();

        //check the summary
        System.out.println(trainDF.structure());
        System.out.println(trainDF.summary());
        System.out.println("Number of clean records" + trainDF.size());

        // EDA
        EDA(trainDF);

        //Modelling
        RandomForest trainModel = RandomForest.fit(Formula.lhs("Survived"),trainDF);  //Left Hand
        System.out.println("feature importance:");
        System.out.println(Arrays.toString(trainModel.importance()));
        System.out.println(trainModel.metrics ());



        //Test

        //EDA
        //First Print Summary
        System.out.println(testDF.summary());

        //Data Cleaning
        //Convert Sex to numeric Values
        testDF = testDF.merge(IntVector.of("Gender", encodeCategory (testDF, "Sex")));

        //dropping unneeded columns
        testDF = testDF.drop("Name");
        testDF = testDF.drop("Sex");

        //delete nulls
        testDF = testDF.omitNullRows();

        //check the summary
        System.out.println(testDF.structure());
        System.out.println(testDF.summary());
        System.out.println("Number of clean records" + testDF.size());




    }

    public static int[] encodeCategory(DataFrame df,String colName){
        String[] values = df.stringVector(colName).distinct().toArray(new String[0]);
        int[] map = df.stringVector(colName).factorize(new NominalScale(values)).toIntArray();
        return map;
    }

    public static void EDA (DataFrame data) throws InvocationTargetException, InterruptedException {
        //get two dataframes of survived and dead
        DataFrame survived = DataFrame.of(data.stream().filter( d -> d.get("Survived").equals(1) ) );
        DataFrame dead = DataFrame.of(data.stream().filter( d -> d.get("Survived").equals(0) ) );


        var survivedAge = survived.stream()
                .mapToDouble(t-> t.isNullAt("Age") ? 0 : t.getDouble("Age"))
                .toArray();

        var deadAge = dead.stream()
                .mapToDouble(t-> t.isNullAt("Age") ? 0 : t.getDouble("Age")).toArray();


        System.out.println("Average Survived Age is " + Arrays.stream(survivedAge).average());
        System.out.println("Average dead Age is " + Arrays.stream(deadAge).average());


        //plotting
        //total
        Histogram.of(data.doubleVector("Age").toDoubleArray(),10,false)
                .canvas()
                .setAxisLabels("Ages","Count")
                .setTitle("Total Ages")
                .window();

        Histogram.of(survivedAge,10,false)
                .canvas()
                .setAxisLabels("Ages","Count")
                .setTitle("Total Ages")
                .window();

        Histogram.of(deadAge,10,false)
                .canvas()
                .setAxisLabels("Ages","Count")
                .setTitle("Total Ages")
                .window();

        ScatterPlot.of(data,"Age","Survived", '*', red ).canvas()
                .setAxisLabels("Age","Survive")
                .setTitle("Age va Survived")
                .window();


    }


}
