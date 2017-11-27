/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.inatel.demos;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.apache.flink.util.Collector;
import java.util.Properties;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GearChanges {

  //private static PrintWriter writer;

  public static void main(String[] args) throws Exception {

    /*try {
      writer = new PrintWriter("/home/aluno/Desktop/log_file.txt", "UTF-8");
    } catch (FileNotFoundException | UnsupportedEncodingException ex) {
      Logger.getLogger(GearChanges.class.getName()).log(Level.SEVERE, null, ex);
    }*/
    
    // Create execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("group.id", "flink_consumer");

    DataStream stream = env.addSource(
            new FlinkKafkaConsumer09<>("flink-demo", 
            new JSONDeserializationSchema(), 
            properties)
    ); 

    stream  .flatMap(new TelemetryJsonParser())
            .keyBy(0)
            .timeWindow(Time.seconds(3))
            .reduce(new Reducer())
            .flatMap(new CountMapper())
            .map(new Printer())
            .print();

    env.execute();

    //writer.close();
  }

    // Receive JSON data from Kafka broker and filter necessary parameters.
    
    // {"Car": 9, "time": "52.196000", "telemetry": {"Vaz": "1.270000", "Distance": "4.605865", "LapTime": "0.128001", 
    // "RPM": "591.266113", "Ay": "24.344515", "Gear": "3.000000", "Throttle": "0.000000", 
    // "Steer": "0.207988", "Ax": "-17.551264", "Brake": "0.282736", "Fuel": "1.898847", "Speed": "34.137680"}}

    static class TelemetryJsonParser implements FlatMapFunction<ObjectNode, Tuple3<String, String, Integer>> {
      @Override
      public void flatMap(ObjectNode jsonTelemetry, Collector<Tuple3<String, String, Integer>> out) throws Exception {
        String carNumber = "car" + jsonTelemetry.get("Car").asText();
        String gear = jsonTelemetry.get("telemetry").get("Gear").asText();
        out.collect(new Tuple3<>(carNumber,  gear, 0 ));
      }
    }

    // This function compare the change of gears and increase the counter.
    // The counter is returned if there was a gear change.
    static class Reducer implements ReduceFunction<Tuple3<String, String, Integer>> {
      @Override
      public Tuple3<String, String, Integer> reduce(Tuple3<String, String,Integer> value1, Tuple3<String, String, Integer> value2) {
        /*if(value1.f0.equals("car4")){
          writeToFile("");
          writeToFile(value1.f0 + " ---> " + value1.f1 + " " + value1.f2);
          writeToFile(value2.f0 + " ---> " + value2.f1 + " " + value2.f2);
        }*/

        if(value1.f1.equals(value2.f1)){
          return new Tuple3<>(value1.f0, value1.f1, value1.f2);
        }else {
          return new Tuple3<>(value1.f0, value2.f1, value1.f2+1);
        }        
      }
    }

    // Receive the last car information and repass to a printer format.
    static class CountMapper implements FlatMapFunction<Tuple3<String, String, Integer>, Tuple2<String, Integer>> {
      @Override
      public void flatMap(Tuple3<String, String, Integer> value, Collector<Tuple2<String, Integer>> out) throws Exception {
        out.collect(  new Tuple2<>( value.f0 , value.f2 )  );
      }
    }

    // Printer
    static class Printer implements MapFunction<Tuple2<String, Integer>, String> {
      @Override
      public String map(Tuple2<String, Integer> value) throws Exception {
        return  String.format("Gear changes of %s : %d", value.f0 , value.f1 ) ;
      }
    }

    // Function used to generate simple log.
    /*private static void writeToFile(String value) {
      writer.println(value);
    }*/

  }