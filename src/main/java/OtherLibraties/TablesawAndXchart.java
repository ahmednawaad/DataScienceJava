package OtherLibraties;

import org.knowm.xchart.*;
import tech.tablesaw.api.Table;

import java.io.IOException;
import java.util.stream.Collectors;

public class TablesawAndXchart {
    public static void main(String[] args) throws IOException {


        // read file
        String path = "src/main/resources/titanic.csv";
        Table titanic_df = Table.read().csv(path);

        System.out.println(titanic_df);
        //get structure of data
        System.out.println(titanic_df.structure());
        //get summary of data
        System.out.println(titanic_df.summary());

        //draw histogram
        System.out.println(titanic_df.column("age"));

        var data = titanic_df.column("age").asList().stream()
                .map(a->a != null ? Double.valueOf(a.toString()) : 0)
                .collect(Collectors.toList());

        Histogram hist2 = new Histogram(data,10);

        CategoryChart chart2 = new CategoryChartBuilder()
                .width(800)
                .height(800)
                .title("Ages")
                .xAxisTitle("Age")
                .yAxisTitle("Count")
                .build();

        //add data to chart
        chart2.addSeries("Chart2",hist2.getxAxisData(),hist2.getxAxisData());
        new SwingWrapper(chart2).displayChart();

        //draw Piechart

        var data2 = titanic_df.column("survived").asList().stream()
                .map(a->a != null ? Double.valueOf(a.toString()) : 0)
                .collect(Collectors.groupingBy(s->s,Collectors.counting()));
        System.out.println(data2);

        PieChart chart3 = new PieChartBuilder().width(800).height(800).title("Survived Va Dead").build();
        chart3.addSeries("Survived",data2.get(1.0));
        chart3.addSeries("Dead",data2.get(0.0));
        new SwingWrapper(chart3).displayChart();

    }
}
