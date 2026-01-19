package com.example.analytics_service.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import notification.Notification;
import notification.NotificationServiceGrpc;

@Service
public class NotificationClient {

    private final NotificationServiceGrpc.NotificationServiceBlockingStub stub;

    public NotificationClient(
            @Value("${notification.host}") String host,
            @Value("${notification.port}") int port
    ) {
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext()
                .build();

        this.stub = NotificationServiceGrpc.newBlockingStub(channel);
    }

    public void notify(Notification.EventMessage msg) {
        stub.notifyEvent(msg);
    }
}
