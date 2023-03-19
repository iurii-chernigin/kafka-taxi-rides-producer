package kafka.producer;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import schemaregistry.FHVRideRecord;
import schemaregistry.GreenRideRecord;

import java.io.FileReader;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.lang.NumberFormatException;

public class FHVRideProducer {

    private Properties kafkaConnectionProps;
    private String dataToProducePath;
    private String FHVRidesTopic;

    public FHVRideProducer(String dataToProducePath, String FHVRidesTopic) {
        this.kafkaConnectionProps = KafkaConfig.getAvroProducerConfig();
        this.dataToProducePath = dataToProducePath;
        this.FHVRidesTopic = FHVRidesTopic;
    }

    /**
     * Publish ride records to Kafka
     *
     * @param rides list of objects where one object contain data about one taxi ride (vendorID, pickupDatetime, etc)
     */
    public void publishRidesToKafka(List<FHVRideRecord> rides) throws ExecutionException, InterruptedException {

        KafkaProducer<String, FHVRideRecord> kafkaProducer = new KafkaProducer<>(kafkaConnectionProps);

        for (FHVRideRecord ride : rides)
        {
            var recordMeta = kafkaProducer.send(
                    new ProducerRecord<>(
                            FHVRidesTopic,
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
    public List<FHVRideRecord> readRideRecords() throws IOException, CsvException {

        var dataFileResource = this.getClass().getResource(dataToProducePath);
        var ridesCSVReader = new CSVReader(new FileReader(dataFileResource.getFile()));

        List<String> csvHeaderList = Arrays.asList(ridesCSVReader.readNext());
        HashMap<String, Integer> fieldToIndexMap = new HashMap<String, Integer>();
        fieldToIndexMap.put("PUlocationID", csvHeaderList.indexOf("PUlocationID"));
        fieldToIndexMap.put("DOlocationID", csvHeaderList.indexOf("DOlocationID"));
        fieldToIndexMap.put("pickup_datetime", csvHeaderList.indexOf("pickup_datetime"));
        fieldToIndexMap.put("dropOff_datetime", csvHeaderList.indexOf("dropOff_datetime"));

        return ridesCSVReader
                .readAll()
                .stream()
                .map(
                        row -> FHVRideRecord
                                .newBuilder()
                                .setPickupLocationId(convertStringToInt(row[fieldToIndexMap.get("PUlocationID")]))
                                .setDropoffLocationId(convertStringToInt(row[fieldToIndexMap.get("DOlocationID")]))
                                .setPickupDatetime(row[fieldToIndexMap.get("pickup_datetime")])
                                .setDropoffDatetime(row[fieldToIndexMap.get("dropOff_datetime")])
                                .setSendedToKafkaTs((int) Instant.now().getEpochSecond())
                                .build())
                .collect(Collectors.toList());
    }

    public Integer convertStringToInt(String str) {
        try {
            return Integer.parseInt(str);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public static void main(String[] args) throws IOException, CsvException, ExecutionException, InterruptedException {
        var fhvRidesProducer = new FHVRideProducer(
                "/sample_fhv_tripdata_2019-01.csv",
                "rides_fhv"
        );
        var rides = fhvRidesProducer.readRideRecords();
        fhvRidesProducer.publishRidesToKafka(rides);
    }
}
