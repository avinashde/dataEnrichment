package com.org.enrich;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

import java.util.Properties;

public interface PipelineInputArgs extends DataflowPipelineOptions {
    /**
     * following are the required input parameter to run the dataflow pipeline
     */

    @Description("kafka hostname")
    @Validation.Required
    String getKafkaHostname();

    void setKafkaHostname(String value);

    @Description("kafka input topic name")
    @Validation.Required
    String getInputTopic();

    void setInputTopic(String value);

    @Description("kafka output topic name")
    @Validation.Required
    String getOutputTopic();

    void setOutputTopic(String value);


}
