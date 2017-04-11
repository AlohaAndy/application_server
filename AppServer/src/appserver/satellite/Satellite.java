package appserver.satellite;

import appserver.job.Job;
import appserver.comm.ConnectivityInfo;
import appserver.job.UnknownToolException;
import appserver.comm.Message;
import static appserver.comm.MessageTypes.JOB_REQUEST;
import static appserver.comm.MessageTypes.REGISTER_SATELLITE;
import appserver.job.Tool;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Hashtable;
import java.util.logging.Level;
import java.util.logging.Logger;
import utils.PropertyHandler;

/**
 * Class [Satellite] Instances of this class represent computing nodes that execute jobs by
 * calling the callback method of tool implementation, loading the tools code dynamically over a network
 * or locally, if a tool got executed before.
 *
 * @author Dr.-Ing. Wolf-Dieter Otte
 */
public class Satellite extends Thread {

    private ConnectivityInfo satelliteInfo = new ConnectivityInfo();
    private ConnectivityInfo serverInfo = new ConnectivityInfo();
    private HTTPClassLoader classLoader = null;
    private Hashtable toolsCache = null;

    public Satellite(){//String satellitePropertiesFile, String classLoaderPropertiesFile, String serverPropertiesFile) {

        // read the configuration information from the file name passed in
        // ---------------------------------------------------------------
        // ...
        
        System.out.println("Created Satellite");
        // create a socket info object that will be sent to the server
        // ...
        
        
        // get connectivity information of the server
        // ...
        
        
        // create class loader
        // -------------------
        // ...
        
        classLoader = new HTTPClassLoader("localhost", 12609);

        // read class loader config
        // ...
        
        
        // get class loader connectivity properties and create class loader
        // ...
        
        
        // create tools cache
        // -------------------
        // ...
        
    }

    @Override
    public void run() {

        try {
            // register this satellite with the SatelliteManager on the server
            // ---------------------------------------------------------------
            // ...
            
            
            // create server socket
            // ---------------------------------------------------------------
            // ...
            ServerSocket server = new ServerSocket(2125);
            
            System.out.println("Listening on port 2125");
            // start taking job requests in a server loop
            // ---------------------------------------------------------------
            // ...
            while(true)
            {
                Socket connection = server.accept();
                SatelliteThread thread = new SatelliteThread(connection, this);
                thread.run();
            }
            
        } catch (IOException ex) {
            Logger.getLogger(Satellite.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    // inner helper class that is instanciated in above server loop and processes job requests
    private class SatelliteThread extends Thread {

        Satellite satellite = null;
        Socket jobRequest = null;
        ObjectInputStream readFromNet = null;
        ObjectOutputStream writeToNet = null;
        Message message = null;

        SatelliteThread(Socket jobRequest, Satellite satellite) {
            this.jobRequest = jobRequest;
            this.satellite = satellite;
            
            System.out.println("Got job request!");
        }

        @Override
        public void run() {
            try {
                // setting up object streams
                // ...
                writeToNet = new ObjectOutputStream(jobRequest.getOutputStream());
                readFromNet = new ObjectInputStream(jobRequest.getInputStream());
                
                // reading message
                // ...
                message = (Message) readFromNet.readObject();
                
                // processing message
                switch (message.getType()) {
                    case JOB_REQUEST:
                        // ...
                        Job job = (Job) message.getContent();
                        System.out.println(job.getToolName());
                        Class jobclass = satellite.classLoader.findClass(job.getToolName());
                        Tool tool = (Tool) jobclass.newInstance();
                        Object result = tool.go(job.getParameters());
                        
                        writeToNet.writeObject(result);
                        break;
                        
                    default:
                        System.err.println("[SatelliteThread.run] Warning: Message type not implemented: " + message.getType());
                }
            } catch (IOException ex) {
                Logger.getLogger(Satellite.class.getName()).log(Level.SEVERE, null, ex);
            } catch (ClassNotFoundException ex) {
                Logger.getLogger(Satellite.class.getName()).log(Level.SEVERE, null, ex);
            } catch (InstantiationException ex) {
                Logger.getLogger(Satellite.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IllegalAccessException ex) {
                Logger.getLogger(Satellite.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        
        private byte[] readFromInput() throws IOException
        {
            int bytesRead = 0;
            int offset = 0;
            int blockSize = 128;
            // read the bytes of the class file ...
            byte[] data = new byte[blockSize];

            while (true) {
                bytesRead = readFromNet.read(data, offset, blockSize);

				// End-of-Stream reached
                // is EOS reached immediately, the file is empty or not there
                if (bytesRead == -1) {
                    // EOS or no data
                    break;
                }

                offset += bytesRead;

                // enlarge field, if necessary
                if (offset + blockSize >= data.length) {
                    byte[] temp = new byte[data.length * 2];
                    System.arraycopy(data, 0, temp, 0, offset);
                    data = temp;
                }
            }

            // cut field to proper size
            if (offset < data.length) {
                byte[] temp = new byte[offset];
                System.arraycopy(data, 0, temp, 0, offset);
                data = temp;
            }
            return data;
        }
    }

    /**
     * Aux method to get a tool object, given the fully qualified class string
     *
     */
    public Tool getToolObject(String toolClassString) throws UnknownToolException, ClassNotFoundException, InstantiationException, IllegalAccessException {

        Tool toolObject = null;

        // ...
        
        return toolObject;
    }

    public static void main(String[] args) {
        // start a satellite
        Satellite satellite = new Satellite();//args[0], args[1], args[2]);
        satellite.run();
        
        //(new Satellite("Satellite.Earth.properties", "WebServer.properties")).start();
        //(new Satellite("Satellite.Venus.properties", "WebServer.properties")).start();
        //(new Satellite("Satellite.Mercury.properties", "WebServer.properties")).start();
    }
}
