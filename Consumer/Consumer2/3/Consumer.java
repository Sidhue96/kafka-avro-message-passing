import java.util.*;
import org.apache.kafka.clients.consumer.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.*;                                
public class Consumer{
    public static void main(String[] args) throws Exception{
        String topicName = "Female";                                            
        String groupName = "FemaleGroup";
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", groupName);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("schema.registry.url", "http://localhost:8081");
        props.put("specific.avro.reader", "true");     
        KafkaConsumer<String, Practice> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topicName));
        try{
            while (true){
                ConsumerRecords<String, Practice> records = consumer.poll(100);
                for (ConsumerRecord<String, Practice>record : records){
                    saveAndCommit(record);
                }
            }
        }catch(Exception ex){
            ex.printStackTrace();
        }
        finally{
            consumer.close();
        }
    }
    private static void saveAndCommit(ConsumerRecord<String, Practice> record){
        Connection con = null;
        Statement stmt = null;
        try{
            Class.forName("org.postgresql.Driver");
            con = DriverManager
            .getConnection("jdbc:postgresql://localhost:5432/healthcare",
            "postgres", "123");
            con.setAutoCommit(false);
            System.out.println("Opened database successfully");
            stmt = con.createStatement();
            String sql = "INSERT INTO female (id,pid,gender,age,hypertension,heart_disease,ever_married,work_type,residence_type,avg_glucose_level,bmi,smoking_status,stroke,time_stamp) "
                            + "VALUES ('"+record.value().getId()+"','"
                            +record.value().getPid()+"','"
                            +record.value().getGender()+"','"
                            +record.value().getAge()+"','"
                            +record.value().getHypertension()+"','"
                            +record.value().getHeartDisease()+"','"
                            +record.value().getEverMarried()+"','"
                            +record.value().getWorkType()+"','"
                            +record.value().getResidenceType()+"','"
                            +record.value().getAvgGlucoseLevel()+"','"
                            +record.value().getBmi()+"','"
                            +record.value().getSmokingStatus()+"','"
                            +record.value().getStroke()+"','"
                            +record.value().getTimeStamp()+"');";
            System.out.println(sql);
            stmt.executeUpdate(sql);
            stmt.close();
            con.commit();
            con.close();
        }
        catch(Exception e){
            System.err.println( e.getClass().getName()+": "+ e.getMessage() );
            System.exit(0);
        }
        System.out.println("Records created successfully");
    }
}