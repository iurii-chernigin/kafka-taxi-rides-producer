package kafka.producer;

import schemaregistry.GreenRideRecord;

import java.util.Properties;
import java.util.List;
import java.util.Arrays;

import com.opencsv.CSVReader;
import java.io.FileReader;

import java.io.IOException;
import com.opencsv.exceptions.CsvException;
import java.util.concurrent.ExecutionException;

public class GreenRideProducer {
    
    private Properties kafkaConnectionProps;
    private String dataToProducePath;

    public GreenRideProducer(String dataToProducePath) {
        this.kafkaConnectionProps = KafkaConfig.getAvroProducerConfig();
        this.dataToProducePath = dataToProducePath;
    }

    public void readRideRecords() {

        var dataFileResource = this.getClass().getResource(dataToProducePath);
        try (CSVReader ridesCSVReader = new CSVReader(new FileReader(dataFileResource.getFile()))) {
            List<String> csvHeaderArr = Arrays.asList(ridesCSVReader.readNext());
            System.out.println(csvHeaderArr);
            System.out.println("Position of DOLocationID is " + csvHeaderArr.indexOf("DOLocationID"));
        } 
        catch (CsvException csvException) {
            csvException.printStackTrace();
        } 
        catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    public static void main(String[] args) {
        var greenRidesProducer = new GreenRideProducer("/sample_green_tripdata_2019-01.csv");
        greenRidesProducer.readRideRecords();
    }

}
