package OtherLibraties;

import joinery.DataFrame;
import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.CategoryChartBuilder;
import org.knowm.xchart.Histogram;
import org.knowm.xchart.SwingWrapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.knowm.xchart.style.Styler.LegendPosition.InsideNW;

public class JoineryAndXchart {
    public static void main(String[] args) throws IOException {

        //read Titanic csv file
        String file_path = "src/main/resources/titanic.csv";
        DataFrame<Object> Titanic_df = DataFrame.readCsv(file_path);

        //print first five records
        System.out.println(Titanic_df.head(5));

        //stats summary
        System.out.println(Titanic_df.describe());

        //dealing with columns
        List<Object> sex = Titanic_df.col("sex");
        Map<Object,Long> mapSex = sex.stream().collect(Collectors.groupingBy(s -> s,Collectors.counting()));
        System.out.println(mapSex);

        //get the unique values
        List<Object> embarked = Titanic_df.col("embarked").stream().distinct().collect(Collectors.toList());
        System.out.println(embarked);

        //drawing Histogram
        //get histogram data
        var data= Titanic_df.col("fare").stream()
                .map(s->s != null ? Double.valueOf(s.toString()):-1)
                .collect(Collectors.toList());


        Histogram hist1 = new Histogram(data,10);
        //building chart
        CategoryChart chart1 = new CategoryChartBuilder()
                .width(800)
                .height(800)
                .title("Fare Histogram")
                .xAxisTitle("Fare")
                .yAxisTitle("Count")
                .build();

        //customize chart
        chart1.getStyler().setLegendPosition(InsideNW);

        //add Series
        chart1.addSeries("Histogram",hist1.getxAxisData(),hist1.getxAxisData());

        //display the chart
        new SwingWrapper(chart1).displayChart();



    }
}
