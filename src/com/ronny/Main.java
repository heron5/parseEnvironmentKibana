package com.ronny;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import org.elasticsearch.client.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileWriter;
import java.io.IOException;  // Import the IOException class to handle errors
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;

import org.apache.http.HttpHost;

public class Main {

    public void updateDb(String source, float temperature, float humidity) {
        SensorProperties sensor = new SensorProperties();
        if (!sensor.getSensorProperties(source))
            return;

        long sourceId = sensor.sourceId;
        String description = sensor.shortDescription;
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss+0200");
        LocalDateTime now = LocalDateTime.now();

        JSONObject jsonBody = new JSONObject();
        JSONObject jsonTags = new JSONObject();
        JSONObject jsonFields = new JSONObject();

        jsonTags.put("source", sourceId);
        jsonTags.put("source_desc", description);
        jsonFields.put("temperature", temperature);
        jsonFields.put("humidity", humidity);

        jsonBody.put("measurement", "EnvironmentLogg");
        jsonBody.put("date", dtf.format(now));
        jsonBody.put("tags", jsonTags);
        jsonBody.put("fields", jsonFields);

        System.out.println(jsonBody);

        HttpHost esHost = new HttpHost("192.168.2.203", 9200);
        RestClient restClient = RestClient.builder(esHost).build();

        Request request = new Request(
                "POST",
                "/environment_index/environmentlog");

        request.setJsonEntity(jsonBody.toString());

        Response response = null;
        try {
            response = restClient.performRequest(request);
            restClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(response);
    }


    public void run() {
        System.out.println("TopicSubscriber initializing...");

        String host = "tcp://kjuladata.se:1883";
        String username = "DataCenter";
        String password = "MightyDatacenter2017";

        try {
            // Create an Mqtt client
            Random rand = new Random();
            MqttClient mqttClient = new MqttClient(host, "parseEnvironment" + rand.toString());
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            connOpts.setUserName(username);
            connOpts.setPassword(password.toCharArray());
            connOpts.setAutomaticReconnect(true);

            // Connect the client
            System.out.println("Connecting to Kjuladata messaging at " + host);
            mqttClient.connect(connOpts);
            System.out.println("Connected");

            // Topic filter the client will subscribe to
            final String subTopic = "logger/environment/#";

            // Callback - Anonymous inner-class for receiving messages
            mqttClient.setCallback(new MqttCallback() {
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    // Called when a message arrives from the server that
                    // matches any subscription made by the client

                    JSONParser parser = new JSONParser();
                    String payLoad = new String(message.getPayload());
                    System.out.println(payLoad);
                    JSONObject json = (JSONObject) parser.parse(payLoad);
                    JSONArray environmentlogg = (JSONArray) json.get("environmentlogg");
                    try {
//                        FileWriter jsonFile = new FileWriter("/Users/f2530720/IdeaProjects/parseEnvironmentKibana/tmp/parseEnvironmentKibana_java.json", false);
                        FileWriter jsonFile = new FileWriter("/tmp/parseEnvironmentKibana.json", false);

                        jsonFile.write(payLoad + "\n");
                        jsonFile.close();
                    } catch (IOException e) {
                        System.err.print("FileWriter went wrong");
                        e.printStackTrace();
                    }

                    try {
                        for (Object logg : environmentlogg) {
                            JSONObject loggbysource = (JSONObject) logg;
                            String source = (String) loggbysource.get("source");
                            float temperature = Float.parseFloat((String) loggbysource.get("temperature"));
                            float humidity = Float.parseFloat((String) loggbysource.get("humidity"));
                            updateDb(source, temperature, humidity);
                        }
                    } catch (Exception pe) {
                        //  System.out.println("position: " + pe.getPosition());
                        System.out.println(pe);
                    }
                }

                public void connectionLost(Throwable cause) {
                    System.out.println("Connection to KjulaData messaging lost!" + cause.getMessage());
                }

                public void deliveryComplete(IMqttDeliveryToken token) {
                }

            });

            // Subscribe client to the topic filter and a QoS level of 0
            System.out.println("Subscribing client to topic: " + subTopic);
            mqttClient.subscribe(subTopic, 0);
            System.out.println("Subscribed");

        } catch (MqttException me) {
            System.out.println("reason " + me.getReasonCode());
            System.out.println("msg " + me.getMessage());
            System.out.println("loc " + me.getLocalizedMessage());
            System.out.println("cause " + me.getCause());
            System.out.println("excep " + me);
            me.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new Main().run();

    }
}
