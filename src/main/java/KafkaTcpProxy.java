import java.io.*;
import java.net.*;

public class KafkaTcpProxy {

    public static void main(String[] args) throws Exception {
        ServerSocket server = new ServerSocket(9092);
        System.out.println("Kafka proxy listening on localhost:9092");

        while (true) {
            Socket client = server.accept();
            Thread worker = new Thread(() -> handle(client));
            worker.setDaemon(true);
            worker.start();
        }
    }

    private static void handle(Socket client) {
        try {
            Socket upstream = new Socket("social-media-kafka", 9092);

            Thread t1 = new Thread(() -> relay(client, upstream));
            Thread t2 = new Thread(() -> relay(upstream, client));

            t1.start();
            t2.start();

            t1.join();
            t2.join();

            client.close();
            upstream.close();

        } catch (Exception e) {
            System.err.println("Proxy error: " + e.getMessage());
            try { client.close(); } catch (IOException ignored) {}
        }
    }

    private static void relay(Socket source, Socket destination) {
        try {
            source.getInputStream().transferTo(
                destination.getOutputStream()
            );
            destination.shutdownOutput();
        } catch (IOException ignored) {}
    }
}
