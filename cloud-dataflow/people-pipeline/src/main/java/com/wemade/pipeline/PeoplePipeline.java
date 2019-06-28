
/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.wemade.pipeline;

import com.google.cloud.teleport.io.WindowedFilenamePolicy;
import com.google.cloud.teleport.util.DurationUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;

import com.google.gson.GsonBuilder;
import com.google.gson.Gson;
import java.util.HashMap;
import java.io.*;
import java.net.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.http.*;
import org.apache.http.client.*;
import org.apache.http.client.config.*;
import org.apache.http.client.methods.*;
import org.apache.http.impl.client.*;
import org.apache.http.impl.conn.*;
import org.apache.http.conn.*;
import org.apache.http.protocol.*;
import org.apache.http.conn.routing.*;
import org.apache.http.util.*;
import org.apache.http.message.*;
import org.apache.http.client.utils.*;
import org.apache.http.entity.*;

import org.json.JSONArray;
import org.json.JSONObject;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * This pipeline ingests incoming data from a Cloud Pub/Sub topic and outputs
 * the raw data into windowed files at the specified output directory.
 *
 * <p>
 * Example Usage:
 *
 * <pre>
 * mvn compile exec:java \
 -Dexec.mainClass=com.google.cloud.teleport.templates.${PIPELINE_NAME} \
 -Dexec.cleanupDaemonThreads=false \
 -Dexec.args=" \
 --project=${PROJECT_ID} \
 --stagingLocation=gs://${PROJECT_ID}/dataflow/pipelines/${PIPELINE_FOLDER}/staging \
 --tempLocation=gs://${PROJECT_ID}/dataflow/pipelines/${PIPELINE_FOLDER}/temp \
 --runner=DataflowRunner \
 --windowDuration=2m \
 --numShards=1 \
 --inputTopic=projects/${PROJECT_ID}/topics/windowed-files \
 --outputDirectory=gs://${PROJECT_ID}/temp/ \
 --outputFilenamePrefix=windowed-file \
 --outputFilenameSuffix=.txt"
 * </pre>
 * </p>
 */
public class PeoplePipeline {

  private static final Logger LOG = LoggerFactory.getLogger(PeoplePipeline.class);

  /**
   * Options supported by the pipeline.
   *
   * <p>
   * Inherits standard configuration options.
   * </p>
   */
  public interface Options extends PipelineOptions, StreamingOptions {
    @Description("The Cloud Pub/Sub topic to read from.")
    @Default.String("projects/wemade-core/topics/steamer-output")
    @Required
    ValueProvider<String> getInputTopic();

    void setInputTopic(ValueProvider<String> value);

    @Description("The Cloud Pub/Sub topic to write to.")
    @Default.String("projects/wemade-core/topics/people-pipeline-output")
    ValueProvider<String> getOutputTopic();

    void setOutputTopic(ValueProvider<String> value);
  }

  /**
   * Main entry point for executing the pipeline.
   * 
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    options.setStreaming(true);

    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  public static PipelineResult run(Options options) {
    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);
    LOG.info("running people pipeline");
    /*
     * Steps: 1) Read string messages from PubSub 2) Window the messages into minute
     * intervals specified by the executor. 3) Output the windowed files to GCS
     */
    pipeline.apply("Read from PubSub", PubsubIO.readStrings().fromTopic(options.getInputTopic()))
        .apply("People Match Key Detection", ParDo.of(new ScoringModelsFn()))
        // parent - I will add
        // class year - gsc will have something
        .apply("Custom Attribute Detection", ParDo.of(new PlaceHolderFn()))
        // assemble the record in the format of Marcus' spreadsheet
        .apply("Package Data", ParDo.of(new PackageDataFn()))
        // upper case
        .apply("Cleansing", ParDo.of(new CleansingFn()))
        // generate a address hash key from upper cased street address, city, state
        .apply("Generate Address Hash", ParDo.of(new AddressHashFn()))
        .apply("Clean up Data", ParDo.of(new CleanUpFn()))
        .apply("Write to PubSub", PubsubIO.writeStrings().to(options.getOutputTopic()));

    // Execute the pipeline and return the result.
    return pipeline.run();
  }

  static class ProcessRecordFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      String message = context.element();
      Gson gson = new GsonBuilder().create();
      HashMap<String, Object> parsedMap = gson.fromJson(message, HashMap.class);
      context.output(gson.toJson(parsedMap));
    }
  }

  private static java.util.ArrayList<String> PIPELINE_OUTPUT_FIELDS = new java.util.ArrayList<String>() {
    {
      add("_wm_owner");
      add("_wm_request");
      add("_wm_row");
      add("_wm_record");
      add("_mk_firstName");
      add("_mk_lastName");
      add("_mk_fullName");
      add("_mk_street");
      add("_mk_street1");
      add("_mk_street2");
      add("_mk_city");
      add("_mk_state");
      add("_mk_postal");
      add("_mk_email");
      add("_mk_customerType");
      add("_mk_organization");
      add("_cf_classYear");
      add("_cf_parent1");
      add("_cf_parent2");
    }
  };

  static class CleanUpFn extends DoFn<String, String> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      String message = context.element();
      Gson gson = new GsonBuilder().create();
      HashMap<String, Object> parsedMap = gson.fromJson(message, HashMap.class);
      java.util.TreeMap<String, Object> outputMap = new java.util.TreeMap<String, Object>(
          new CleanUpKeySorter());

      for (HashMap.Entry<String, Object> pipelineField : parsedMap.entrySet()) {
        String key = pipelineField.getKey();
        if (key.startsWith("_wm") || key.startsWith("_cf") || key.startsWith("_mk")) {
          outputMap.put(key, pipelineField.getValue());
        } else {
          // if (pipelineField.getValue().getClass().getName() == String.class.getName()) {
          //   outputMap.put("_uk_" + key, pipelineField.getValue());
          // } else {
          //   java.util.Map<String, Object> attributes = (java.util.Map<String, Object>) pipelineField.getValue();
          //   if (attributes.containsKey("value")) {
          //     outputMap.put("_raw_" + key, attributes.get("value"));
          //   }
          //   if (attributes.containsKey("model.scorer")) {
          //     outputMap.put("_score_" + key, attributes.get("model.scorer"));
          //   }
          // }
        }
      }
      context.output(gson.toJson(outputMap));
    }

    class CleanUpKeySorter implements java.util.Comparator<String> {

      public CleanUpKeySorter() {
      }

      public int compare(String a, String b) {
        int ai = PIPELINE_OUTPUT_FIELDS.indexOf(a);
        int bi = PIPELINE_OUTPUT_FIELDS.indexOf(b);
        if (ai == -1) {
          ai = 99999;
        }
        if (bi == -1) {
          bi = 99999;
        }
        int result1 = Integer.valueOf(ai).compareTo(Integer.valueOf(bi));
        if (result1 == 0) {
          return a.compareTo(b);
        }
        else {
          return result1;
        }
      }
    }
  }

  static class CustomAttributeFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      String message = context.element();
      Gson gson = new GsonBuilder().create();
      HashMap<String, Object> parsedMap = gson.fromJson(message, HashMap.class);

      context.output(gson.toJson(parsedMap));
    }
  }

  static class AddressHashFn extends DoFn<String, String> {
    private MessageDigest md = null;
    private static String[] HASH_FIELDS = { "_mk_street1", "_mk_city", "_mk_state", "_mk_postal" };

    @StartBundle
    public void startBundle() {
      try {
        md = MessageDigest.getInstance("SHA-256");
      } catch (NoSuchAlgorithmException e) {
      }
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      String message = context.element();
      Gson gson = new GsonBuilder().create();
      HashMap<String, Object> parsedMap = gson.fromJson(message, HashMap.class);

      StringBuilder hashBuilder = new StringBuilder();
      for (String field : HASH_FIELDS) {
        if (parsedMap.containsKey(field) && parsedMap.get(field) != null) {
          hashBuilder.append(parsedMap.get(field));
          hashBuilder.append("|");
        }
      }
      String hashField = hashBuilder.toString();
      parsedMap.put("_wm_addressKey", hashField);
      if (md != null) {
        byte[] messageDigest = md.digest(hashField.getBytes());
        BigInteger no = new BigInteger(1, messageDigest);
        String hashtext = no.toString(16);
        while (hashtext.length() < 32) {
          hashtext = "0" + hashtext;
        }
        parsedMap.put("_wm_addressHash", hashtext);
      }

      context.output(gson.toJson(parsedMap));
    }
  }

  static class PackageDataFn extends DoFn<String, String> {
    private static String[] OUTPUT_FIELDS = { "_mk_firstName", "_mk_lastName", "_mk_fullName", "_mk_street",
        "_mk_street1", "_mk_street2", "_mk_city", "_mk_state", "_mk_postal", "_mk_email", "_mk_customerType",
        "_mk_organization", "_cf_classYear", "_cf_parent1", "_cf_parent2" };
    private static java.util.Map<String, String> MAP_SCORER = java.util.Map.ofEntries(
        new java.util.AbstractMap.SimpleEntry<String, String>("_mk_street1", "label_AD1"),
        new java.util.AbstractMap.SimpleEntry<String, String>("_mk_street2", "label_AD2"),
        new java.util.AbstractMap.SimpleEntry<String, String>("_mk_city", "label_CITY"),
        new java.util.AbstractMap.SimpleEntry<String, String>("_mk_email", "label_EMAIL"),
        new java.util.AbstractMap.SimpleEntry<String, String>("_mk_firstName", "label_FNAME"),
        new java.util.AbstractMap.SimpleEntry<String, String>("_mk_lastName", "label_LNAME"),
        new java.util.AbstractMap.SimpleEntry<String, String>("_mk_state", "label_ST"),
        new java.util.AbstractMap.SimpleEntry<String, String>("_mk_postal", "label_ZIP"));

    @ProcessElement
    public void processElement(ProcessContext context) {
      
      String message = context.element();
      LOG.info("package data received " + message);
      Gson gson = new GsonBuilder().create();
      HashMap<String, Object> parsedMap = gson.fromJson(message, HashMap.class);

      for (String outputField : OUTPUT_FIELDS) {
        String outputValue = "";
        if (MAP_SCORER.containsKey(outputField)) {
          String scorerKey = MAP_SCORER.get(outputField);
          double score = 0.01;
          for (HashMap.Entry<String, Object> pipelineField : parsedMap.entrySet()) {
            String key = pipelineField.getKey();
            if (!key.startsWith("_wm") && !key.startsWith("_mk") && !key.startsWith("_cf")) {
              java.util.Map<String, Object> attributes = (java.util.Map<String, Object>) pipelineField.getValue();
              if (attributes.containsKey("model.scorer")) {
                String scoreJson = (String) attributes.get("model.scorer");
                java.util.Map<String, Object> scores = (java.util.Map<String, Object>) gson.fromJson(scoreJson,
                    java.util.Map.class);

                if (scores.containsKey(scorerKey)) {
                  double scoreValue = (double) scores.get(scorerKey);
                  if (scoreValue > score) {
                    outputValue = (String) attributes.get("value");
                    score = scoreValue;
                  }
                }
              }
            }
          }
        }
        parsedMap.put(outputField, outputValue);
      }

      context.output(gson.toJson(parsedMap));
    }
  }

  static class CleansingFn extends DoFn<String, String> {
    private static String[] UPPERCASE_FIELDS = { "_mk_street", "_mk_city", "_mk_state", "_mk_postal" };

    @ProcessElement
    public void processElement(ProcessContext context) {
      String message = context.element();
      Gson gson = new GsonBuilder().create();
      HashMap<String, Object> parsedMap = gson.fromJson(message, HashMap.class);
      // upper case
      for (String field : UPPERCASE_FIELDS) {
        if (parsedMap.containsKey(field) && parsedMap.get(field) != null) {
          parsedMap.put(field, parsedMap.get(field).toString().toUpperCase());
        }
      }
      context.output(gson.toJson(parsedMap));
    }
  }

  static class ScoringModelsFn extends DoFn<String, String> {
    @ProcessElement
    @SuppressWarnings("unchecked")
    public void processElement(ProcessContext context) {
      LOG.info("running people pipiline ScoringModel");
      Gson gson = new GsonBuilder().create();
      HashMap<String, Object> map = (HashMap<String, Object>) gson.fromJson(context.element(), HashMap.class);
      HashMap<String, Object> output = new HashMap<String, Object>();
      for (HashMap.Entry<String, Object> entry : map.entrySet()) {
        String key = entry.getKey();
        Object value = entry.getValue();
        if (key.startsWith("_wm")) {
          output.put(key, value);
        } else {
          if (value.getClass().equals(String.class)) {
            HashMap<String, Object> valueMap = new HashMap<String, Object>();
            valueMap.put("value", (String) value);
            CloseableHttpResponse httpRes = null;
            CloseableHttpClient client = HttpClients.createDefault();
            String addressModel = "";
            String peopleModel = "";
            String resp = "";
            try {

              URI uri = new URIBuilder().setScheme("http").setHost("34.68.24.100").setPath("/api/usaddress")
                  .setParameter("address", (String) value).build();
              HttpGet getRequest = new HttpGet(uri);

              CloseableHttpResponse res = client.execute(getRequest);
              HttpEntity entity = res.getEntity();

              if (entity != null) {
                try {
                  resp = EntityUtils.toString(entity);
                } finally {
                  EntityUtils.consume(entity);
                  res.close();
                }
              }
            } catch (Exception e) {
              LOG.warn("failed by get response from map server with retries for datamade usaddress " + value);
              LOG.warn("usaddress exception " + e.getMessage() + ", " + e.toString());
            }

            if (resp != null && resp != "") {
              addressModel = resp.replace("\n", "");
            }

            resp = "";
            try {
              URI uri = new URIBuilder().setScheme("http").setHost("34.68.24.100").setPath("/api/probablepeople")
                  .setParameter("name", (String) value).build();
              HttpGet getRequest = new HttpGet(uri);

              CloseableHttpResponse res = client.execute(getRequest);
              HttpEntity entity = res.getEntity();

              if (entity != null) {
                try {
                  resp = EntityUtils.toString(entity);
                } finally {
                  EntityUtils.consume(entity);
                  res.close();
                }
              }
            } catch (Exception e) {
              LOG.warn("failed by get response from map server with retries for datamade probablepeople " + value);
              LOG.warn("probablepeople exception " + e.getMessage() + ", " + e.toString());
            }

            if (resp != null && resp != "") {
              peopleModel = resp.replace("\n", "");
            }

            resp = "";
            // scorer
            try {
              JSONArray addressArray = new JSONArray(addressModel);
              JSONArray peopleArray = new JSONArray(peopleModel);

              URIBuilder builder = new URIBuilder().setScheme("http").setHost("34.67.137.20")
              .setPath("/pipeline-scorer/wemade-v1r01").setParameter("row_num", "0").setParameter("variable", key)
              .setParameter("value", (String) value);

              for (int i = 0; i < addressArray.length(); i++) {
                String jsonObject = addressArray.get(i).toString();
                if (jsonObject.startsWith("{")) {
                  JSONObject jsonObj = new JSONObject(jsonObject);
                  java.util.Iterator<String> keys = jsonObj.keys();

                  while(keys.hasNext()) {
                    String resultKey = keys.next();
                    builder.addParameter("address_parse", resultKey);
                  }
                }
                else {
                  builder.addParameter("address_parse", jsonObject.toString());
                }
              }

              for (int i = 0; i < peopleArray.length(); i++) {
                String jsonObject = peopleArray.get(i).toString();
                if (jsonObject.startsWith("{")) {
                  JSONObject jsonObj = new JSONObject(jsonObject);
                  java.util.Iterator<String> keys = jsonObj.keys();

                  while(keys.hasNext()) {
                    String resultKey = keys.next();
                    builder.addParameter("people_parse", resultKey);
                  }
                }
                else {
                  builder.addParameter("people_parse", jsonObject.toString());
                }
              }              

              URI uri = builder.build();
                  // .setParameter("people_parse", (String) valueMap.getOrDefault("model.probablepeople", "{}"))
                  // .setParameter("address_parse", (String) valueMap.getOrDefault("model.usaddress", "{}"))
                  // .build();
              HttpGet getRequest = new HttpGet(uri);

              // // saving the code below for JSON post to scorer
              // HttpPost postRequest = new
              // HttpPost("http://104.197.238.19/pipeline-scorer/wemade-v1r01");
              // HashMap<String, Object> postObject = new HashMap<String, Object>();
              // postObject.put("row_num", 0);
              // postObject.put("variable", key);
              // postObject.put("value", value);
              // postObject.put("people_parse", (HashMap<String, Object>)
              // gson.fromJson((String)valueMap.getOrDefault("model.probablepeople", "{}"),
              // HashMap.class));
              // postObject.put("address_parse", (HashMap<String, Object>)
              // gson.fromJson((String)valueMap.getOrDefault("model.usaddress", "{}"),
              // HashMap.class));
              // String postJson = gson.toJson(postObject);
              // StringEntity postEntity = new StringEntity(postJson);
              // valueMap.put("request.scorer", postJson);
              // postRequest.setEntity(postEntity);
              // postRequest.setHeader("Accept", "application/json");
              // postRequest.setHeader("Content-type", "application/json");

              CloseableHttpResponse res = client.execute(getRequest);
              HttpEntity entity = res.getEntity();

              if (entity != null) {
                try {
                  resp = EntityUtils.toString(entity);
                } finally {
                  EntityUtils.consume(entity);
                  res.close();
                }
              }
            } catch (Exception e) {
              LOG.warn("failed by get response from map server with retries for scorer " + value);
              LOG.warn("scorer exception " + e.getMessage() + ", " + e.toString());
            }

            if (resp != null && resp != "") {
              try {
                HashMap<String, Object> scores = (HashMap<String, Object>) gson.fromJson(resp.replace("\n", ""),
                    HashMap.class);
                for (HashMap.Entry<String, Object> score : scores.entrySet()) {
                  if (score.getKey().startsWith("label")) {
                    scores.put(score.getKey(), Double.valueOf((String) score.getValue()).doubleValue());
                  }
                }
                valueMap.put("model.scorer", gson.toJson(scores));
              }
              catch (Exception ex) {
                if (valueMap.containsKey("error.scorer")) {
                  valueMap.put("error.scorer", (String)valueMap.get("error.scorer") + ";" + resp);
                }
                else {
                  valueMap.put("error.scorer", resp);
                }
              }
            }

            output.put(key, valueMap);
          }
        }
        // ...
      }
      context.output(gson.toJson(output));
    }

    // private static CloseableHttpClient _httpClient = null;
    // private static PoolingHttpClientConnectionManager _connManager = null;
    // private static HttpHost _host = new HttpHost("35.194.9.115", 80);

    // private static HttpRequestRetryHandler _RetryHandler = null;

    private java.text.DecimalFormat decimalFormat = (java.text.DecimalFormat) java.text.DecimalFormat
        .getInstance(java.util.Locale.US);

    // @StartBundle
    // public void startBundle() {
    //   // Instantiate your external service client (Static if threadsafe)
    //   LOG.info("Setting up HttpClient");
    //   int timeout = 2000;
    //   int maxConnection = 20;
    //   _connManager = new PoolingHttpClientConnectionManager();
    //   _connManager.setMaxPerRoute(new HttpRoute(_host), maxConnection);

    //   RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(timeout)
    //       .setConnectionRequestTimeout(timeout).setSocketTimeout(timeout).build(); // config retry

    //   _RetryHandler = new HttpRequestRetryHandler() {
    //     public boolean retryRequest(IOException exception, int executionCount, HttpContext context) {
    //       LOG.info(exception.toString());
    //       LOG.info("try request: " + executionCount);
    //       if (executionCount >= 5) {
    //         // Do not retry if over max retry count
    //         return false;
    //       }
    //       if (exception instanceof InterruptedIOException) {
    //         // Timeout
    //         return false;
    //       }
    //       if (exception instanceof UnknownHostException) {
    //         // Unknown host
    //         return false;
    //       }
    //       if (exception instanceof ConnectTimeoutException) {
    //         // Connection refused
    //         return false;
    //       }
    //       if (exception instanceof javax.net.ssl.SSLException) {
    //         // SSL handshake exception
    //         return false;
    //       }
    //       return true;
    //     }
    //   };

    //   _httpClient = HttpClients.custom().setConnectionManager(_connManager).setDefaultRequestConfig(requestConfig)
    //       .setRetryHandler(_RetryHandler).build();
    //   LOG.info("Setting up HttpClient is done.");
    // }

    // @FinishBundle
    // public void finishBundle() {
    //   // Shutdown your external service client if needed
    //   LOG.info("Tearing down HttpClient and Connection Manager.");
    //   try {
    //     _httpClient.close();
    //     _connManager.close();
    //   } catch (Exception e) {
    //     LOG.warn(e.toString());
    //   }
    //   LOG.info("HttpClient and Connection Manager have been teared down.");
    // }
  }

  static class ColumnIdentificationFn extends DoFn<String, String> {
    @ProcessElement
    @SuppressWarnings("unchecked")
    public void processElement(ProcessContext context) {
      Gson gson = new GsonBuilder().create();
      HashMap<String, Object> parsed = (HashMap<String, Object>) gson.fromJson(context.element(), HashMap.class);

      context.output(gson.toJson(parsed));
    }
  }

  static class PlaceHolderFn extends DoFn<String, String> {
    @ProcessElement
    @SuppressWarnings("unchecked")
    public void processElement(ProcessContext context) {
      Gson gson = new GsonBuilder().create();
      HashMap<String, Object> parsed = (HashMap<String, Object>) gson.fromJson(context.element(), HashMap.class);

      context.output(gson.toJson(parsed));
    }
  }

  static class HashMapToJsonFn extends DoFn<HashMap<String, Object>, String> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      Gson gson = new GsonBuilder().create();
      context.output(gson.toJson(context.element()));
    }
  }
}
