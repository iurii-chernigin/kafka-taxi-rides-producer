package kafka.producer;

import schemaregistry.GreenRideRecord;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Instant;
import java.util.Properties;
import java.util.List;
import java.util.Arrays;
import java.util.HashMap;

import com.opencsv.CSVReader;
import java.io.FileReader;

import java.io.IOException;
import com.opencsv.exceptions.CsvException;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;


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
     * @param rides list of objects where one object contain data about one taxi ride (vendorID, pickupDatetime, etc)
     */
    public void publishRidesToKafka(List<GreenRideRecord> rides) throws ExecutionException, InterruptedException {

        KafkaProducer<String, GreenRideRecord> kafkaProducer = new KafkaProducer<>(kafkaConnectionProps);

        for (GreenRideRecord ride : rides)
        {
            var recordMeta = kafkaProducer.send(
                    new ProducerRecord<>(
                            greenRidesTopic,
                            String.valueOf(ride.getPickupLocationId()),
                            ride
                    ),
                    (metadata, exception) -> {
                        if (exception != null) {
                            System.out.println(exception.getMessage());
                        }
                    }
            );
            System.out.println("\nPickupLocationID: " + ride.getPickupLocationId());
            System.out.println("Offset: " + recordMeta.get().offset());
            System.out.println("Partition: " + recordMeta.get().partition());
            Thread.sleep(500);
        }

        kafkaProducer.close();
    }

    /**
     * Read csv with green taxi rides and publish them to Kafka topic
     *
     * Fields: VendorID, lpep_pickup_datetime, lpep_dropoff_datetime, PULocationID, DOLocationID
     * Generated field: sended_to_kafka_ts
     */
    public List<GreenRideRecord> readRideRecords() throws IOException, CsvException {

        var dataFileResource = this.getClass().getResource(dataToProducePath);
        var ridesCSVReader = new CSVReader(new FileReader(dataFileResource.getFile()));

        List<String> csvHeaderList = Arrays.asList(ridesCSVReader.readNext());
        HashMap<String, Integer> fieldToIndexMap = new HashMap<String, Integer>();
        fieldToIndexMap.put("PULocationID", csvHeaderList.indexOf("PULocationID"));
        fieldToIndexMap.put("DOLocationID", csvHeaderList.indexOf("DOLocationID"));
        fieldToIndexMap.put("VendorID", csvHeaderList.indexOf("VendorID"));
        fieldToIndexMap.put("lpep_pickup_datetime", csvHeaderList.indexOf("lpep_pickup_datetime"));
        fieldToIndexMap.put("lpep_dropoff_datetime", csvHeaderList.indexOf("lpep_dropoff_datetime"));

        return ridesCSVReader
                .readAll()
                .stream()
                .map(
                        row -> GreenRideRecord
                                .newBuilder()
                                .setVendorId(row[fieldToIndexMap.get("VendorID")])
                                .setPickupLocationId(Integer.parseInt(row[fieldToIndexMap.get("PULocationID")]))
                                .setDropoffLocationId(Integer.parseInt(row[fieldToIndexMap.get("DOLocationID")]))
                                .setPickupDatetime(row[fieldToIndexMap.get("lpep_pickup_datetime")])
                                .setDropoffDatetime(row[fieldToIndexMap.get("lpep_dropoff_datetime")])
                                .setSendedToKafkaTs((int) Instant.now().getEpochSecond())
                                .build())
                .collect(Collectors.toList());
    }

    public static void main(String[] args) throws IOException, CsvException, ExecutionException, InterruptedException {
        var greenRidesProducer = new GreenRideProducer(
                "/sample_green_tripdata_2019-01.csv",
                "rides_green"
        );
        var rides = greenRidesProducer.readRideRecords();
        greenRidesProducer.publishRidesToKafka(rides);
    }

}
