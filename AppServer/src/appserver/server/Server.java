package appserver.server;

import appserver.comm.Message;
import static appserver.comm.MessageTypes.JOB_REQUEST;
import static appserver.comm.MessageTypes.REGISTER_SATELLITE;
import appserver.comm.ConnectivityInfo;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Properties;
import utils.PropertyHandler;

/**
 *
 * @author Dr.-Ing. Wolf-Dieter Otte
 */
public class Server {

    // Singleton objects - there is only one of them. For simplicity, this is not enforced though ...
    static SatelliteManager satelliteManager = null;
    static LoadManager loadManager = null;
    static ServerSocket serverSocket = null;

    
    private ConnectivityInfo serverInfo = new ConnectivityInfo();

    public Server(String serverPropertiesFile) {

        // create satellite and load managers
        // ...
        satelliteManager = new SatelliteManager();
        loadManager = new LoadManager();
        
        // read server port from server properties file
        int serverPort = 0;
        PropertyHandler serverConfiguration = null;
        try {
            serverConfiguration = new PropertyHandler(serverPropertiesFile);
        } catch (Exception e) {
            // no use carrying on, so bailing out...
            e.printStackTrace();
            System.exit(1);
        }

        serverInfo.setHost(serverConfiguration.getProperty("HOST"));
        serverInfo.serverPort(Integer.parseInt(serverConfiguration.getProperty("PORT")));
        
        // create server socket
        // ...
        try {
            serverSocket = new ServerSocket(serverInfo.getPort());
        } catch (IOException ex) {
            System.err.println("[Satellite.run] Could not create server socket");
            ex.printStackTrace();
            System.exit(1);
        }
        
    }

    public void run() {
    // start serving clients in server loop ...
    // ...
        
         while(true) {
            try {
                (new ServerThread(serverSocket.accept())).start();
            } catch (IOException ex) {
                System.err.println("[Server.run] Warning: Error accepting client");
            }
        }
    }

    // objects of this helper class communicate with clients
    private class ServerThread extends Thread {

        Socket client = null;
        ObjectInputStream readFromNet = null;
        ObjectOutputStream writeToNet = null;
        Message message = null;

        private ServerThread(Socket client) {
            this.client = client;
        }

        @Override
        public void run() {
            // setting up object streams
            // ...
            try {
                readFromNet = new ObjectInputStream(client.getInputStream());
                writeToNet = new ObjectOutputStream(client.getOutputStream());
            } catch (IOException ex) {
                System.err.println("[ServerThread.run] Failed to set up object streams.");
                ex.printStackTrace();
                System.exit(1);
            }
            
            
            // reading message
            try {
                message = (Message) readFromNet.readObject();
            } catch (Exception e) {
                System.err.println("[ServerThread.run] Message could not be read from object stream.");
                e.printStackTrace();
                System.exit(1);
            }

            // processing message
            ConnectivityInfo satelliteInfo = null;
            String satelliteName = null;
            
            switch (message.getType()) {
                case REGISTER_SATELLITE:
                    // read satellite info
                    satelliteInfo = (ConnectivityInfo) message.getContent();
                    satelliteName = satelliteInfo.getName();
                    
                    // register satellite
                    synchronized (Server.satelliteManager) {
                        Server.satelliteManager.registerSatellite(satelliteInfo);
                    }

                    // add satellite to loadManager
                    synchronized (Server.loadManager) {
                        Server.loadManager.satelliteAdded(satelliteName);
                    }

                    break;

                case JOB_REQUEST:
                    System.err.println("\n[ServerThread.run.JOB_REQUEST] Received job request");

                    satelliteName = null;
                    synchronized (Server.loadManager) {
                        // get next satellite from load manager
                        String nextSatellite = Server.loadManager.nextSatellite();
                        
                        // get connectivity info for next satellite from satellite manager
                        ConnectivityInfo nextSatelliteInfo = Server.satelliteManager.getSatelliteForName(nextSatellite);
                        
                    }

                    Socket satellite = null;
                    // connect to satellite
                    try {
                        satellite = new Socket(satelliteInfo.getPort());
                    } catch (Exception ex) {
                        System.err.println("[ServerThread.run.JOB_REQUEST] Could not create server socket");
                        ex.printStackTrace();
                        System.exit(1);
                    }
                    
                    // open object streams,
                    try {
                        // todo: need move the tryt objectoutstream to be declear out sie of the try
                        ObjectOutputStream writeToSatellite = new ObjectOutputStream(satellite.getInputStream());
                        // todo: nee to get output the stream
                        ObjectInputStream readFromSatellite = new ObjectInputStream(satellite.getInputStream());
                    } catch (IOException ex) {
                        System.err.println("[ServerThread.run.JOB_REQUEST] Failed to set up object streams.");
                        ex.printStackTrace();
                        System.exit(1);
                    }
                    // forward message (as is) to satellite,
                    try {
                        writeToSatellite.writeObject(message);
                    } catch (Exception ex) {
                        System.err.println("[ServerThread.run.JOB_REQUEST] Error when writing object to output stream (writeToSatellite).");
                        ex.printStackTrace();
                        System.exit(1);
                    }
                    // receive result from satellite and
                    try {
                        Object result = readFromSatellite.readObject();
                    } catch (Exception ex) {
                        System.err.println("[ServerThread.run.JOB_REQUEST] Error when reading object from output stream (readFromSatellite).");
                        ex.printStackTrace();
                        System.exit(1);
                    }
                    // write result back to client
                    try {
                        writeToNet.writeObject(result);
                    } catch (Exception ex) {
                        System.err.println("[ServerThread.run.JOB_REQUEST] Error when writing object to output stream (writeToNet).");
                        ex.printStackTrace();
                        System.exit(1);
                    }

                    break;

                default:
                    System.err.println("[ServerThread.run] Warning: Message type not implemented");
            }
        }
    }

    // main()
    public static void main(String[] args) {
        // start the application server
        Server server = null;
        if(args.length == 1) {
            server = new Server(args[0]);
        } else {
            server = new Server("../../config/Server.properties");
        }
        server.run();
    }
}
