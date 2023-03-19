package kafka.producer;

import schemaregistry.GreenRideRecord;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Instant;
import java.util.Properties;
import java.util.List;
import java.util.Arrays;

import com.opencsv.CSVReader;
import java.io.FileReader;

import java.io.IOException;
import com.opencsv.exceptions.CsvException;
import java.util.concurrent.ExecutionException;


/**
 * Class for producing and sending events from csv file to Kafka topic
 * @author iurii.chernigin
 */
public class GreenRideProducer {
    
    private Properties kafkaConnectionProps;
    private String dataToProducePath;
    private String greenRidesTopic;

    public GreenRideProducer(String dataToProducePath, String greenRidesTopic) {
        this.kafkaConnectionProps = KafkaConfig.getAvroProducerConfig();
        this.dataToProducePath = dataToProducePath;
        this.greenRidesTopic = greenRidesTopic;
    }

    /**
     * Publish ride records to Kafka
     *
     * @param rideRecord object contains data about one taxi ride (vendorID, pickupDatetime, etc)
            */
    public void publishRidesToKafka(KafkaProducer kafkaProducer, GreenRideRecord rideRecord) throws ExecutionException, InterruptedException {

        var recordMeta = kafkaProducer.send(
                new ProducerRecord<>(
                        greenRidesTopic,
                        String.valueOf(rideRecord.getPickupLocationId()),
                        rideRecord
                ),
                (metadata, exception) -> {
                    if (exception != null) {
                        System.out.println(exception.getMessage());
                    }
                }
        );
        System.out.println(rideRecord.getPickupLocationId());
    }

    /**
     * Read csv with green taxi rides and publish them to Kafka topic
     *
     * Fields: VendorID, lpep_pickup_datetime, lpep_dropoff_datetime, PULocationID, DOLocationID
     * Generated field: sended_to_kafka_ts
     */
    public void readRideRecords() throws IOException, CsvException {

        KafkaProducer<String, GreenRideRecord> kafkaProducer = new KafkaProducer<>(kafkaConnectionProps);
        var dataFileResource = this.getClass().getResource(dataToProducePath);
        var ridesCSVReader = new CSVReader(new FileReader(dataFileResource.getFile()));

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
                .forEach(row -> {
                    try {
                        publishRidesToKafka(kafkaProducer, row);
                    } catch (ExecutionException e) {
                        throw new RuntimeException(e);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });

        kafkaProducer.close();

    }

    public static void main(String[] args) throws IOException, CsvException {
        var greenRidesProducer = new GreenRideProducer(
                "/sample_green_tripdata_2019-01.csv",
                "rides_green"
        );
        greenRidesProducer.readRideRecords();
    }

}
