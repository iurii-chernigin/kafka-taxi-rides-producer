package kafka.producer;

import schemaregistry.GreenRideRecord;

import java.time.Instant;
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

    public void publishRidesToKafka(GreenRideRecord rideRecord) {
        // Left field naming from source to be consistent,
        // transformation will be at next steps
        System.out.println(rideRecord.getDropoffLocationId());

    }

    /*
     * Read csv with green taxi rides and publish them to Kafka topic
     *
     * Fields: VendorID, lpep_pickup_datetime, lpep_dropoff_datetime, PULocationID, DOLocationID
     * Generated field: sended_to_kafka_ts
     */
    public void readRideRecords() {

        var dataFileResource = this.getClass().getResource(dataToProducePath);
        try (CSVReader ridesCSVReader = new CSVReader(new FileReader(dataFileResource.getFile()))) {


            List<String> csvHeaderList = Arrays.asList(ridesCSVReader.readNext());
            Integer pickupLocationIndex = csvHeaderList.indexOf("PULocationID");
            Integer dropoffLocationIndex = csvHeaderList.indexOf("DOLocationID");
            Integer vendorIDIndex = csvHeaderList.indexOf("VendorID");
            Integer pickupDatetimeIndex = csvHeaderList.indexOf("lpep_pickup_datetime");
            Integer dropoffDatetimeIndex = csvHeaderList.indexOf("lpep_dropoff_datetime");

            ridesCSVReader
                    .readAll()
                    .stream()
                    .map(
                            row -> GreenRideRecord
                                    .newBuilder()
                                    .setVendorId(row[vendorIDIndex])
                                    .setPickupLocationId(Integer.parseInt(row[pickupLocationIndex]))
                                    .setDropoffLocationId(Integer.parseInt(row[dropoffLocationIndex]))
                                    .setPickupDatetime(row[pickupDatetimeIndex])
                                    .setDropoffDatetime(row[dropoffDatetimeIndex])
                                    .setSendedToKafkaTs((int) Instant.now().getEpochSecond())
                                    .build()
                    )
                    .forEach(row -> publishRidesToKafka(row));

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
