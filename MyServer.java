import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

import org.eclipse.paho.client.mqttv3.*;

public class MyServer {

	// Init server
	public static void main(String[] args) {
		connectToServer();
	}

	// Server stuff
	public static void connectToServer() {
		
		// serverstuff
		System.out.println("Setting the Server info...");
		
		System.out.println("Setting Broker IP...");
		String broker = "tcp://localhost:1883";
		System.out.println("Setting client ID for Mqtt client...");
		String clientId = "JavaServer";
		MqttClientPersistence persistence = null;   //means just memory persistence

		
		String serverUrl;
		MqttAsyncClient sampleClient;

		// topics
		System.out.println("Setting the topics...");
		
		String publishTopicAndroid = "pub_android";
		String publishTopicIot = "pub_iot";
		String subscribeTopicAndroid = "sub_android";
		String subscribeTopicIot = "sub_iot";

        	System.out.println("Setting topics for the android and iot to subscribe and publish to...");
        
		try {
			sampleClient = new MqttAsyncClient(broker, clientId, persistence);
			MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true); //no persistance -- when i disconnect i will automatically unsubscribbe
            connOpts.setMaxInflight(3000);

			connOpts.setUserName("emqx_test");
            connOpts.setPassword("emqx_test_password".toCharArray());

            //sampleClient.setCallback(messageArrived);
            System.out.println("Connecting to broker: " + broker);
            (sampleClient.connect(connOpts)).waitForCompletion();
            System.out.println(" Connected ");
            int[] qualities = {0, 0};
            sampleClient.subscribe(subscribeTopicAndroid, 0);
			sampleClient.subscribe(subscribeTopicIot, 0);
            //System.out.println("subscribing to topics " + subTopics[0] + " and " + subTopics[1]);
            //System.out.println("publishing to topic " + pubTopic1);
            //System.out.println("publishing to topic " + pubTopic2);
        }
        catch (MqttException me) {
            System.out.println("reason "+me.getReasonCode());
            System.out.println("msg "+me.getMessage());
            System.out.println("loc "+me.getLocalizedMessage());
            return ;
        }
		finally {

		}

            //Create Input&Outputstreams for the connection
           /*
            InputStream inputToServer = connectionSocket.getInputStream();
            OutputStream outputFromServer = connectionSocket.getOutputStream();

            Scanner scanner = new Scanner(inputToServer, "UTF-8");
            PrintWriter serverPrintOut = new PrintWriter(new OutputStreamWriter(outputFromServer, "UTF-8"), true);

            serverPrintOut.println("Hello World! Enter Peace to exit.");

            //Have the server take input from the client and echo it back
            //This should be placed in a loop that listens for a terminator text e.g. bye
            boolean done = false;

            while(!done && scanner.hasNextLine()) {
                String line = scanner.nextLine();
                serverPrintOut.println("Echo from <Your Name Here> Server: " + line);

                if(line.toLowerCase().trim().equals("peace")) {
                    done = true;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }*/
	}

	void messageArrived(String topic, MqttMessage message) throws MqttException {
		System.out.println(String.format("[%s] %s", topic, new String(message.getPayload())));
		System.out.println("\tMessage published on topic 'Area1'");
	}
}
