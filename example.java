package Server;

import Heatmap.CsvScanner;
import Heatmap.MapSquare;
import Heatmap.Point;
//import com.sun.prism.shader.Solid_TextureYV12_AlphaTest_Loader;
import Plot.DistanceErrorGraph;
import Plot.PlotData;
import org.eclipse.paho.client.mqttv3.*;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.ui.RefineryUtilities;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Vector;


public class MqttServer {
    private MapSquare[][] mapSquares;
    private  String broker = "tcp://broker.hivemq.com:1883";
    static private String[] subTopics;
    private  String pubTopic1;
    private  String pubTopic2;
    static int messages_completed=0;
    MqttAsyncClient sampleClient;
    private int predictedTimestep;

    private Dictionary<String,String> data;
    private Statement statement;

    public static final String EMPTY = "\"nothing_to_predict\"";

    public Vector<Double> vehicle26Distances;
    public Vector<Double> vehicle27Distances;

    PlotData plotData;

    XYSeries series;


    public MqttServer(MapSquare[][] mapSquare){

        mapSquares = mapSquare;
        predictedTimestep = -1;
        data = new Hashtable<>();
        statement = initializeDB();
        vehicle26Distances = new Vector<>();
        vehicle27Distances = new Vector<>();
        series = new XYSeries("timestep");
        plotData = new PlotData();
    }

    public void updateVehiclePlotValues(String vehicle){
        plotData.update(vehicle,data,predictedTimestep);
    }

    public void addDistance(double distance, String vehicle){
        if(vehicle.equals("26")) {
            vehicle26Distances.add(distance);
        }
        else if(vehicle.equals("27")) {
            vehicle27Distances.add(distance);
        }
        else
            System.out.println("Invalid Vehicle number! (add distance)");
        data.put("error",String.valueOf(distance*1000));
        updateVehiclePlotValues(vehicle);
    }

    public double getMeanDistance(String vehicle){
        Vector<Double> temp;
        if(vehicle.equals("26"))
            temp = vehicle26Distances;
        else if(vehicle.equals("27"))
            temp = vehicle27Distances;
        else{
            System.out.println("(Mean) wrong vehicle id");
            return -1;
        }
        int i;
        double sum = 0;
        for(i=0; i<temp.size(); i++) {
            sum += temp.get(i);
        }
        if(temp.size() == 0)
            return 0;
        else
            return sum/(double)temp.size();

    }

    public void sendPredictedLocation(String lon, String lat, String timestep){
        /* First checks in which map square the predicted point is.
        * then obtains the rssi from the cell and publishes the data
        * to the android client. */
        Point predicted_point = new Point(Double.parseDouble(lon),Double.parseDouble(lat));
        int row,col;
        for(row=0; row<4; row++){
            for(col=0; col<10; col++){
                mapSquares[row][col].squareBounds();
                if(mapSquares[row][col].hasPoint(predicted_point)){
                    System.out.println("("+row+","+col+")");
                    double rssi = mapSquares[row][col].calculateRssiMean();
                    double throughput = rssi/2;
                    data.put("predicted_RSSI",String.valueOf(rssi));
                    data.put("predicred_throughput",String.valueOf(throughput));
                    String id = data.get("id");
                    String data = "<"+timestep +"," + id + "," + lon + "," + lat + "," + rssi + "," + throughput+">"; // send as csv line in order to use csvScanner class on android to extract values
                    MqttMessage predictedData = new MqttMessage(data.getBytes());
                    try {
                        System.out.println("Sending ..."+lon+","+lat);
                        String pubTopic;
                        if(id.contains("27"))
                            pubTopic = pubTopic2;
                        else
                            pubTopic = pubTopic1;

                        sampleClient.publish(pubTopic, predictedData);
                        return;
                    }catch (MqttException e){
                        e.printStackTrace();
                    }
                }
            }
        }
    }


    public int getPredictedTimestep(){return predictedTimestep;}

    public void setPredictedTimestep(int timestep){predictedTimestep = timestep;}

    public Dictionary<String,String> getData(){return data;}

    public void saveSimulationData(String csvLine){
        System.out.println("Saving...");
        CsvScanner scanner = new CsvScanner();
        data.put("timestep",scanner.get_attr(csvLine,"timestep"));
        data.put("id",scanner.get_attr(csvLine,"id"));
        data.put("real_lat",scanner.get_attr(csvLine,"lat"));
        data.put("real_long",scanner.get_attr(csvLine,"lon"));
        double rssi = Double.parseDouble(scanner.get_attr(csvLine,"rssi"));
        data.put("real_rssi", Double.toString(rssi));
        double throughput = rssi/2;
        data.put("real_throughput", Double.toString(throughput));
    }


    public void savePredictedData(String pred_lat, String pred_long){
        data.put("predicted_lat",pred_lat);
        data.put("predicted_long",pred_long);
    }

    public String getAttr(String line,String attr){
       // System.out.println("get attr "+line+"   "+attr);
        /*if(line.contains("end"))
            return "-1";*/
        CsvScanner scanner = new CsvScanner();
        return scanner.get_attr(line,attr);
    }


    public Statement initializeDB(){
        final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
        final String DB_URL ="jdbc:mysql://localhost/project";

        final String USER = "root";
        final String PASS = "toor";

        Connection conn = null;
        Statement statement = null;
        try{
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(DB_URL,USER,PASS);
            statement = conn.createStatement();
            return statement;
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return null;
    }


    public void insertDataToDB(){

        //System.out.println(lat1+" "+lon1+" "+lat2+ " "+lon2);
        String Id = data.get("id");
        String sql = "INSERT INTO Data (timestep, device_id, real_lat, real_long, predicted_lat, predicted_long, real_RSSI, real_throughput, predicted_RSSI, predicted_throughput)"+
                "VALUES("+data.get("timestep")+","+data.get("id")+","+data.get("real_lat")+","+data.get("real_long")+","+data.get("predicted_lat")+","+data.get("predicted_long")+
                ","+data.get("real_rssi")+","+data.get("real_throughput")+","+data.get("predicted_RSSI")+","+data.get("predicted_throughput")+")";
        System.out.println("Executing update: "+sql);
        try {
            if(statement == null){
                System.out.println("Statement is null!!! Cannot execute sql query.");
                return;
            }
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        // Calculate distance
        String pred_lat = data.get("predicted_lat");
        if(!pred_lat.contains("nothing")) {
            double lat1 = Double.parseDouble((String) data.get("real_lat"));
            double lon1 = Double.parseDouble((String) data.get("real_long"));
            double lat2 = Double.parseDouble(pred_lat);
            double lon2 = Double.parseDouble((String) data.get("predicted_long"));
            DistanceCalculator distanceCalculator = new DistanceCalculator();
            addDistance(distanceCalculator.distance(new Point(lat1, lon1), new Point(lat2, lon2), "K"), Id); // K is for kilometers
        }
    }


    public boolean clientHasFinished(String msg){
        return msg.startsWith("end");
    }

    public void ShowStatistics(String msg){
        String id = msg.substring(msg.indexOf("+")+1);
        if(id.equals("27")){
         //   vehicle27RealLontituteDataset.addSeries(series);
            data.put("meanError27",String.valueOf(getMeanDistance(id)*1000));
            plotData.finish(id,data);
            plotData.plotLatitudes(id);
            plotData.plotError(id);
           /* DistanceErrorGraph vehicle26RealLonGraph = new DistanceErrorGraph("Vehicle26","Real Lontitude Graph","real lontitude",vehicle27RealLontituteDataset);
            vehicle26RealLonGraph.pack();
            RefineryUtilities.centerFrameOnScreen(vehicle26RealLonGraph);
            vehicle26RealLonGraph.setVisible(true);*/
        }
        else if(id.equals("26")){
            data.put("meanError26",String.valueOf(getMeanDistance(id)*1000));
            plotData.finish(id,data);
            plotData.plotLatitudes(id);
            plotData.plotError(id);
        }
        System.out.println("Mean is: "+getMeanDistance(id)*1000 +" meters.");

    }

    public void runServer() throws IOException {
        System.out.println("Starting server...");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("Give Broker IP Address, or use \"d\" for default (broker.hivemq.com)");

        String ans = reader.readLine();
        if(!ans.trim().equals("d"))
            broker = "tcp://" + ans.trim() +":1883";

        System.out.println("Give two topics to subscribe, or press \"d\" to use default settings.");
        subTopics = new String[2];
        ans = reader.readLine();
        if(!ans.trim().equals("d") && !ans.trim().equals("")) {
            subTopics[0] = ans;
            System.out.println("Now the second...");
            subTopics[1] = reader.readLine();
        }
        else{
            subTopics[0] = "vehicle_pub_26";
            subTopics[1] = "vehicle_pub_27";
        }
        System.out.println("Enter two pub topics now, or press \"d\" to use default settings. Default is: \"JavaServerPub\"");
        ans = reader.readLine();
        String ans2 = reader.readLine();
        if(!ans.trim().equals("d") && !ans.trim().equals("") && !ans2.trim().equals("d") && !ans2.trim().equals("")){
            pubTopic1 = ans;
            pubTopic2 = ans2;
        }
        else {
            pubTopic1 = "JavaServer26";
            pubTopic2 = "JavaServer27";
        }
        System.out.println("SUB: topic1: "+subTopics[0]+" topic2: "+subTopics[1]+" PUB1: "+pubTopic1);
        System.out.println("SUB: topic1: "+subTopics[0]+" topic2: "+subTopics[1]+" PUB2: "+pubTopic2);
        String clientId = "JavaServer";
        MqttClientPersistence persistence = null;   //means just memory persistence

        try {
            sampleClient = new MqttAsyncClient(broker, clientId, persistence);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true); //no persistance -- when i disconnect i will automatically unsubscribbe
            connOpts.setMaxInflight(3000);
            sampleClient.setCallback(mqttCallback);
            System.out.println("Connecting to broker: " + broker);
            (sampleClient.connect(connOpts)).waitForCompletion();
            System.out.println(" Connected ");
            int[] qualities = {0, 0};
            sampleClient.subscribe(subTopics, qualities);
            System.out.println("subscribing to topics " + subTopics[0] + " and " + subTopics[1]);
            System.out.println("publishing to topic " + pubTopic1);
            System.out.println("publishing to topic " + pubTopic2);


        }
        catch (MqttException me) {
            System.out.println("reason "+me.getReasonCode());
            System.out.println("msg "+me.getMessage());
            System.out.println("loc "+me.getLocalizedMessage());
            return ;
        }

    }



    MqttCallback mqttCallback = new MqttCallback() { // this was static before
        @Override
        public void connectionLost(Throwable cause) {
            return;
        }


        /* currentTimestep is initialised with -1. So the first time we save just the real data.
          Then predicts the next data and stores them in a Dictionary. Then the current timestep
          is set to be the predicted one. e.g. When client sends csv line with timestep 122, current
          timestep is set as 123. Then predicted data are sent to the android client and are also saved
          on a dictionary on server side, so they can be extracted on next message arrival and be stored
          in the database where the distance calculation takes place also. (All these occur in the else block!).
          When client stops sending data, server finds the mean of the distances between real and predicted
          data and prints them. :)
         */


        @Override
        public void messageArrived(String currTopic, MqttMessage message) throws MqttException {
            String msg = new String(message.getPayload());
            System.out.println("Topic:" + currTopic + " Message:" + msg + " qos" + message.getQos());

            if(clientHasFinished(msg)) // if client transmission is over, show distance mean etc...
                ShowStatistics(msg);
            else {
                saveSimulationData(msg);
                if((predictedTimestep == Integer.parseInt(getAttr(msg,"timestep")))){
                    insertDataToDB();
                }
                // Check if real simulation data came back after predicted ones.
                CoursePrediction coursePrediction = new CoursePrediction();
                Dictionary<String,String> predictedData = coursePrediction.predictedPosition(msg);
                if (predictedData != null) {
                    String pred_lon = predictedData.get("predicted_lon");
                    String pred_lat = predictedData.get("predicted_lat");
                    String timestep = predictedData.get("timestep");
                    System.out.println("timestep: " + timestep);
                    setPredictedTimestep(Integer.parseInt(timestep));
                    // Send predicted location to android client
                    Point point = new Point(Double.parseDouble(pred_lon),Double.valueOf(pred_lat));
                    if(point.isValid())
                        sendPredictedLocation(pred_lon,pred_lat,timestep);
                    savePredictedData(pred_lat, pred_lon);
                }
            }
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken token) {
            try {
                if(token.getMessage()==null){
                    messages_completed++;
                //    System.out.println("Messages completed:" + messages_completed);
                }
                //null means that message was delivered
            }
            catch(MqttException e){
                e.getMessage();
            }
        }
    };
}
