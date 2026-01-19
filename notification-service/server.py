from concurrent import futures

import time
import grpc

import notification_pb2
import notification_pb2_grpc

class NotificationService(notification_pb2_grpc.NotificationServiceServicer):
    def NotifyEvent(self, request, context):
        print ("Primljena notifikacija")
        print (f"type={request.type}")
        print (f"deviceId={request.deviceId}")
        print (f"value={request.value}")
        print (f"location={request.location}")
        print (f"ts={request.ts}")

        return notification_pb2.NotifyResponse(ok=True, message="ok")
    

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    notification_pb2_grpc.add_NotificationServiceServicer_to_server(NotificationService(),server)
    server.add_insecure_port("[::]:50051")
    server.start()
    print("Notification service listening on :50051")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        server.stop(0)

if __name__=="__main__":
    serve()