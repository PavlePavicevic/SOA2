package com.example.analytics_service.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.example.analytics_service.model.Event;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;

@Service
public class InfluxWriter {

    private final WriteApiBlocking writeApi;

    public InfluxWriter(
            @Value("${influx.url}") String url,
            @Value("${influx.token}") String token,
            @Value("${influx.org}") String org,
            @Value("${influx.bucket}") String bucket
    ) {
        System.out.println("InfluxWriter bean created: " + this.getClass() + " @" + System.identityHashCode(this));
        InfluxDBClient client = InfluxDBClientFactory.create(url, token.toCharArray(), org, bucket);
        this.writeApi = client.getWriteApiBlocking();
    }

    public void write(Event e) {
        System.err.println("### INFLUX WRITE HIT ###");
        Point p = Point.measurement("detected_event")
                .addTag("deviceId", e.deviceId)
                .addTag("type", e.type)
                .addTag("location", e.location)
                .addField("value", e.value)
                .time(e.ts, WritePrecision.MS);
        try{

            System.out.println("WRITING TO INFLUX: " + p.toLineProtocol());
            writeApi.writePoint(p);
            System.out.println("WRITE API CALLED");
        }
        catch (Exception ex){
            ex.printStackTrace();
        }
    }
}
