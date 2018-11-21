import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import org.apache.kafka.clients.producer.*;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
public class HCProducer {                                                 
    public static void main(String[] args) throws Exception{
        String csvFile = "healthcare.csv";
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";
        String topicName = "";
        String key = "";
        DateFormat df = new SimpleDateFormat("E, MMM dd yyyy HH:mm:ss");
        Date date = new Date();
        Practice value;
        try{
            String dateString = df.format(date);
            br = new BufferedReader(new FileReader(csvFile));
            while((line = br.readLine())!= null){
                String[] practice = line.split(cvsSplitBy);
                topicName = practice[2];
                key = practice[0];
                value = new Practice(practice[0],practice[1],practice[2],practice[3],practice[4],practice[5],practice[6],practice[7],practice[8],practice[9],practice[10],practice[11],practice[12],dateString);
                Properties props = new Properties();
                props.put("bootstrap.servers", "localhost:9092");
                props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
                props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
                props.put("schema.registry.url", "http://localhost:8081");
                Producer<String,Practice> producer = new KafkaProducer<>(props);
                ProducerRecord<String,Practice> record = new ProducerRecord<>(topicName,key,value);
                producer.send(record, new MyProducerCallback());
                producer.close();
            }
        }
        catch(FileNotFoundException e){
            e.printStackTrace();
        }
        catch(IOException e){
            e.printStackTrace();
        }
        finally{
            if(br!=null){
                try{
                    br.close();
                }
                catch(IOException e){
                    e.printStackTrace();
                }
            }
        }
    }
}
class MyProducerCallback implements Callback{
    @Override
    public  void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (e != null) System.out.println("AsynchronousProducer failed with an exception");
    }
}