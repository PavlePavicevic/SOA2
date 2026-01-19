package com.example.analytics_service.service;

import com.example.analytics_service.model.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import notification.Notification;
import notification.NotificationServiceGrpc;
import org.eclipse.paho.client.mqttv3.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class MqttEventSubscriber {

    private final ObjectMapper mapper = new ObjectMapper();
    private final InfluxWriter influxWriter;
    private final NotificationClient notificationClient;

    @Value("${mqtt.broker}") private String broker;
    @Value("${mqtt.topic}") private String topic;
    @Value("${mqtt.clientId}") private String clientId;
    @Value("${app.significantType}") private String significantType;

    public MqttEventSubscriber(
            InfluxWriter influxWriter,
            NotificationClient notificationClient
    ) {
        this.influxWriter = influxWriter;
        this.notificationClient = notificationClient;
    }

    @PostConstruct
    public void start() throws Exception {
        System.out.println("=== START MARKER ===");

        System.out.println(
            "MQTT CONFIG -> broker=" + broker +
            ", topic=" + topic +
            ", clientId=" + clientId
        );

        MqttClient client = new MqttClient(broker, clientId);

        MqttConnectOptions opts = new MqttConnectOptions();
        opts.setAutomaticReconnect(true);
        opts.setCleanSession(true);

        client.connect(opts);

        client.subscribe(topic, (t, msg) -> {
            String payload = new String(msg.getPayload());
            System.out.println("EVENT RECEIVED RAW: " + payload);

            Event e;
            try {
                System.out.println("ABOUT TO PARSE JSON");
                e = mapper.readValue(payload, Event.class);
                System.out.println("JSON PARSED SUCCESSFULLY");
                System.out.println("EVENT PARSED: " + e);
            } catch (Exception ex) {
                System.err.println("JSON PARSE FAILED");
                ex.printStackTrace();
                return;
            }


            try {
                influxWriter.write(e);
                System.out.println("INFLUX WRITE CALLED");
            } catch (Exception ex) {
                System.err.println("INFLUX WRITE FAILED");
                ex.printStackTrace();
            }
        });

    
        
        System.out.println("Using influxWriter: " + influxWriter.getClass() + " @" + System.identityHashCode(influxWriter));
        System.out.println("Subscribed to " + topic + " @ " + broker);
    }
}
