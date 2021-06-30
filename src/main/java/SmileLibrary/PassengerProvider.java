package SmileLibrary;


import org.apache.commons.csv.CSVFormat;
import smile.data.DataFrame;
import smile.data.Tuple;
import smile.io.Read;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.stream.Collectors;

public class PassengerProvider {

    private DataFrame passengerDataFrame;

    public DataFrame readCSVFile(String path) throws IOException, URISyntaxException {
        CSVFormat format = CSVFormat.DEFAULT.withFirstRecordAsHeader();
        passengerDataFrame = Read.csv(path,format);
        passengerDataFrame = passengerDataFrame.select("Name", "Pclass", "Age", "Sex", "Survived");
        System.out.println("Loaded Successfully");
        System.out.println(passengerDataFrame.structure());
        return passengerDataFrame;
    }
    public DataFrame readCSVTestFile(String path) throws IOException, URISyntaxException {
        CSVFormat format = CSVFormat.DEFAULT.withFirstRecordAsHeader();
        passengerDataFrame = Read.csv(path,format);
        passengerDataFrame = passengerDataFrame.select("Name", "Pclass", "Age", "Sex");
        System.out.println("Loaded Successfully");
        System.out.println(passengerDataFrame.structure());
        return passengerDataFrame;
    }

    public DataFrame getPassengerDataFrame() {
        return passengerDataFrame;
    }

    public List<Passenger> getListOfPassenger(DataFrame data){
        List<Passenger> passengerList = new ArrayList<>();
        assert passengerDataFrame != null;

        ListIterator<Tuple> listIterator = data.stream().collect(Collectors.toList()).listIterator();
        while (listIterator.hasNext()){
            Tuple row = listIterator.next();
            passengerList.add(new Passenger(
                    ((Integer) row.get("Class")),
                    ((Integer) row.get("Survived")),
                    ((String) row.get("Name")),
                    ((String) row.get("Sex")),
                    ((Double) row.get("Age")),
                    ((Integer) row.get("SibSp")),
                    ((Integer) row.get("Parch")),
                    ((String) row.get("Ticket")),
                    ((Double) row.get("Fare")),
                    ((String) row.get("Cabin")),
                    ((String) row.get("Embarked")),
                    ((String) row.get("Destination")),
                    ((String) row.get("Lifeboat")),
                    ((Integer) row.get("Body"))
            ));

        }
        return passengerList;

    }


}
