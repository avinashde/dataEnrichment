package com.org.enrich;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.*;

/**
 * step1: read user id from kafka topic
 * step2: extract user ids from kafka data
 * step3: form the dyanamic postgresql query and fetch the data from the postgesql table
 * step4: write user data back to kafka topic
 * */


public class dataEnrichment {
    private static final Logger LOG = LoggerFactory.getLogger(dataEnrichment.class);

    public static void run(PipelineInputArgs options)
            throws IOException, IllegalArgumentException {
        FileSystems.setDefaultPipelineOptions(options);

        /**
         * Create the Pipeline object with the options we defined above.
         */
        Pipeline pipeline = Pipeline.create(options);

        /**
         * Read data from kafka topic
         * */

        PCollection<KV<String, String>> collection = pipeline.apply(KafkaIO.<String, String>read()
                .withBootstrapServers(options.getKafkaHostname())
                .withTopic(options.getInputTopic())
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withoutMetadata());

        /**
         * call ParDo function ProcessKafkaFn()
         * */
        PCollection<KV<String, String>> getUserdata =
                collection.apply("ReadFromKafka", ParDo.of(new ProcessKafkaFn()));


        /**
         * write user data to kafka topic
         * */
        getUserdata.apply("WriteToKafka",
                KafkaIO.<String, String>write()
                        .withBootstrapServers(
                                options.getKafkaHostname())
                        .withTopic(options.getOutputTopic())
                        .withKeySerializer(StringSerializer.class)
                        .withValueSerializer(StringSerializer.class));

        pipeline.run().waitUntilFinish();
    }

    public static void main(String[] args) throws IOException, IllegalArgumentException {
        // Create and set your PipelineOptions.
        PipelineOptionsFactory.register(PipelineInputArgs.class);
        PipelineInputArgs options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(PipelineInputArgs.class);
        run(options);
    }

    static class ProcessKafkaFn
            extends DoFn<KV<String, String>, KV<String, String>>  {

        @ProcessElement
        public void processElement(ProcessContext context) throws IOException {
            /**
             * read postgres sql required parameters
             * */
            FileInputStream fis=new FileInputStream("psql.properties");
            Properties p=new Properties();
            p.load (fis);
            String dname= (String) p.get ("Dname");
            String hostname= (String) p.get ("Hostname");
            String username= (String) p.get ("Uname");
            String password= (String) p.get ("Password");
            String driver= (String) p.get ("Driver");

            /**
             * step1:read user id from kafka
             * */

            JSONObject jsonRecord = new JSONObject(context.element().getValue());
            JSONObject UserID = jsonRecord.getJSONObject("Users");
            JSONArray Ids = UserID.getJSONArray("Userid");
            List<String> list = new ArrayList<String>();
            for(int i = 0; i < Ids.length(); ++i) {
                JSONObject rec = Ids.getJSONObject(i);
                Integer id = rec.getInt("id");
                String ids = Integer.toString(id);
                list.add(ids);
            }

            /**
             * form postgresql dynamic query to fetch user data
             * */
            Connection c = null;
            Statement stmt = null;
            try {
                String connString = "jdbc:postgresql:"+hostname+"/"+dname;
                System.out.println(connString);

                Class.forName(driver);
                c = DriverManager
                        .getConnection(connString,
                                username, password);
                c.setAutoCommit(false);
                LOG.info("Opened database successfully");
                stmt = c.createStatement();
                StringBuilder strbul=new StringBuilder();
                for(String str : list)
                {
                    strbul.append(str);
                    //for adding comma between elements
                    strbul.append(",");
                }
                //just for removing last comma
                strbul.setLength(strbul.length()-1);
                String str=strbul.toString();

                LOG.info("Converted String is " + str);


                try {
                        String selectClause ="SELECT json_agg(quantiphi)::jsonb FROM quantiphi where user_id in" +"("+str+")";
                        ResultSet enrichResult = stmt.executeQuery(selectClause);
                        while (enrichResult.next()) {
                             //Read values using column name
                            int i =1;
                            String finalUserData = enrichResult.getString(1);
                            String JsonData ="{\"User\":"+finalUserData+"}";
                            context.output(KV.of(null,JsonData));
                            enrichResult.close();
                        }
                    } catch (SQLException ex) {
                        ex.printStackTrace();
                    }

                stmt.close();
                c.close();
            }
            catch ( Exception e ) {
                System.err.println( e.getClass().getName()+": "+ e.getMessage() );
                System.exit(0);
            }
            LOG.info("Operation done successfully");


        }

        }
}