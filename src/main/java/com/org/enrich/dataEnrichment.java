package com.org.enrich;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.*;
import java.util.*;

/**
 * step1: read data from kafka topic
 * step2: extract user ids from kafka data
 * step3: read user ids from postgresql database
 * step4: find common user id from kafka and postgresql data
 * step5: form the dyanamic postgresql query and fetch the data based on common user ids
 * step6: write user data to kafka topic
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

        PCollection<KV<String,String>> fixedWindowedItems = collection.apply(
                Window.<KV<String,String>>into(FixedWindows.of(Duration.standardSeconds(15))));



        /**
         * call ParDo function ProcessKafkaFn()
         * */
        PCollection<KV<String, String>> getUserdata =
                fixedWindowedItems.apply("ReadFromKafka", ParDo.of(new ProcessKafkaFn()));


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
             * read user id from context
             * read user id from postgresql
             * find the common user ids from both the input source like kafka and postgresql
             * form the dynamic postgresql query to read the data from the postgresql
             * send user data back  DoFn  {context.output}
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
            KV<String, String> element = context.element();
            JSONObject ExtractUserid = new JSONObject(element);
            Integer userIdFromKafka = (Integer) ExtractUserid.get("User_Id");
            String id = Integer.toString(userIdFromKafka);
            ArrayList<String> userId= new ArrayList<>();
            userId.add(id);

            /**
             * step1:read user id from postgresql
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
                ResultSet results = stmt.executeQuery("select user_id from quantiphi");
                /**
                 * Stores properties of a ResultSet object, including column count
                 * */
                ResultSetMetaData rsmd = results.getMetaData();
                int columnCount = rsmd.getColumnCount();
                ArrayList<String> resultList= new ArrayList<>(columnCount);
                while (results.next()) {
                    int i = 1;
                    while(i <= columnCount) {
                        resultList.add(results.getString(i++));
                    }
                }

                /**
                 *  find the common user ids from both the input source like kafka and postgresql
                 * */

                List<String> commonIDs = new ArrayList<>(resultList);
                commonIDs.retainAll(userId);

                for (int counter = 0; counter < commonIDs.size(); counter++) {
                    LOG.info(commonIDs.get(counter));
                    String selectClause = "SELECT json_agg(quantiphi)::jsonb FROM quantiphi where user_id="+commonIDs.get(counter);
                    try {
                        ResultSet enrichResult = stmt.executeQuery(selectClause);

                        while (enrichResult.next()) {
                            // Read values using column name
                            String finalUserData = enrichResult.getString(1);
                            context.output(KV.of(null,finalUserData));
                            LOG.info(finalUserData);
                    }
                    } catch (SQLException ex) {
                        ex.printStackTrace();
                    }

                    /**
                     * send user data back  {context.output}
                     * */

                }
                results.close();
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