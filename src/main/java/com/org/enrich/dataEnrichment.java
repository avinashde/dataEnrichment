package com.org.enrich;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.sql.*;
import java.util.*;

/**
 * step1: read user id from kafka topic
 * step2: extract user ids from kafka data
 * step3: form the dyanamic postgresql query and fetch the data from the postgesql table
 * step4: merge two Pcollection
 * step4: send user data back to kafka topic
 * */


public class dataEnrichment {
    PsqlProperties properties = new PsqlProperties();
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
         * call ParDo function ProcessKafkaUser() for user entity and call ProcessKafkaIp() Pardo function for ip entity
         * */

        PCollection<String> getUserData =
                collection.apply("ReadFromKafka", ParDo.of(new ProcessKafkaUser()));

        PCollection<String> geIpData =
                collection.apply("ReadFromKafka", ParDo.of(new ProcessKafkaIp()));


        /**
         * merged getUserData and geIpData collection and send merged user/ip data back to kafka topic
         * */

        PCollectionList<String> pcs = PCollectionList.of(getUserData).and(geIpData);
        PCollection<String> merged = pcs.apply(Flatten.<String>pCollections());
        merged.apply(ParDo.of(new DoFn<String,KV<String,String>>() {
            @ProcessElement
            public void processElement(ProcessContext context){
                context.output(KV.of(null, context.element()));
            }
        })).apply("WriteToKafka",
                KafkaIO.<String, String> write()
                        .withBootstrapServers(
                                options.getKafkaHostname())
                        .withTopic(options.getOutputTopic())
                        .withKeySerializer(StringSerializer.class)
                        .withValueSerializer(StringSerializer.class));



        pipeline.run().waitUntilFinish();
    }

    public static void main(String[] args) throws IOException, IllegalArgumentException {
        PsqlProperties properties = new PsqlProperties();
        // Create and set your PipelineOptions.
        PipelineOptionsFactory.register(PipelineInputArgs.class);
        PipelineInputArgs options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(PipelineInputArgs.class);
        run(options);
    }

    static class ProcessKafkaUser
            extends DoFn<KV<String, String>,String> {

        @ProcessElement
        public void processElement(ProcessContext context) throws IOException {
            /**
             * step1:read user id from kafka
             * */
            JSONObject jsonRecord = new JSONObject(context.element().getValue());
            JSONObject UserID = jsonRecord.getJSONObject("Users");
            JSONArray Ids = UserID.getJSONArray("User");
            List<String> UserIdList = new ArrayList<String>();
            for (int i = 0; i < Ids.length(); ++i) {
                JSONObject rec = Ids.getJSONObject(i);
                Integer id = rec.getInt("id");
                String ids = Integer.toString(id);
                UserIdList.add(ids);
            }

            /**
             * form postgresql dynamic query to fetch user data
             * */
            Connection c = null;
            Statement stmt = null;
            try {
                PsqlProperties properties = new PsqlProperties();
                Class.forName(properties.getProp().get(3));
                c = DriverManager.getConnection(properties.getProp().get(0),properties.getProp().get(1), properties.getProp().get(2));
                c.setAutoCommit(false);
                LOG.info("Opened database successfully");
                stmt = c.createStatement();
                StringBuilder strbul = new StringBuilder();
                for (String str : UserIdList) {
                    strbul.append(str);
                    //for adding comma between elements
                    strbul.append(",");
                }
                //just for removing last comma
                strbul.setLength(strbul.length() - 1);
                String str = strbul.toString();

                LOG.info("Converted String is " + str);


                try {
                    String selectClause = "SELECT json_agg(quantiphi)::jsonb FROM quantiphi where user_id in" + "(" + str + ")";
                    ResultSet enrichResult = stmt.executeQuery(selectClause);
                    while (enrichResult.next()) {
                        //Read values using column name
                        int i = 1;
                        String finalUserData = enrichResult.getString(1);
                        String JsonData = "{\"User\":" + finalUserData + "}";
                        context.output(JsonData);

                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }

                stmt.close();
                c.close();
            } catch (Exception e) {
                System.err.println(e.getClass().getName() + ": " + e.getMessage());
                System.exit(0);
            }
            LOG.info("Operation done successfully");


        }

    }

    static class ProcessKafkaIp
            extends DoFn<KV<String, String>,String> {

        @ProcessElement
        public void processElement(ProcessContext context) throws IOException, ClassNotFoundException {
            /**
             * step1:read user id from kafka
             * */

            JSONObject jsonRecord = new JSONObject(context.element().getValue());
            JSONObject UserID = jsonRecord.getJSONObject("Ips");
            JSONArray Ids = UserID.getJSONArray("Ip");
            List<String> Iplist = new ArrayList<String>();
            for (int i = 0; i < Ids.length(); ++i) {
                JSONObject rec = Ids.getJSONObject(i);
                String ip = rec.getString("ip");
                Iplist.add(ip);
            }

            /**
             * form postgresql dynamic query to fetch user data
             * */
            Connection c = null;
            Statement stmt = null;
            try {
                PsqlProperties properties = new PsqlProperties();
                Class.forName(properties.getProp().get(3));
                c = DriverManager.getConnection(properties.getProp().get(0),properties.getProp().get(1), properties.getProp().get(2));
                c.setAutoCommit(false);
                LOG.info("Opened database successfully");
                stmt = c.createStatement();
                StringBuilder strbul = new StringBuilder();
                for (String str : Iplist) {
                    strbul.append(str);
                    //for adding comma between elements
                    strbul.append(",");
                    //just for removing last comma
                    strbul.setLength(strbul.length() - 1);
                    String string = "'" + Iplist.toString().replace("[", "").replace("]", "").replace(" ", "").replace(",", "','") + "'";
                    System.out.println("s is" + string);

                    LOG.info("Converted String is " + string);

                    try {
                        String selectClause = "SELECT json_agg(ipdetails)::jsonb FROM ipdetails where ip in" + "(" + string + ")";
                        ResultSet enrichResult = stmt.executeQuery(selectClause);
                        while (enrichResult.next()) {
                            //Read values using column name
                            int i = 1;
                            String finalUserData = enrichResult.getString(1);
                            String JsonData = "{\"Ips\":" + finalUserData + "}";
                            System.out.println(JsonData);
                            context.output(JsonData);

                        }
                    } catch (SQLException ex) {
                        ex.printStackTrace();
                    }

                    stmt.close();
                    c.close();
                }
                LOG.info("Operation done successfully");


            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
