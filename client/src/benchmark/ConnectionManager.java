package benchmark;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.voltdb.*;
import org.voltdb.client.*;

public class ConnectionManager {

    ArrayList<String> initialServerList = new ArrayList<String>();
    Client client;
    ScheduledExecutorService executor;

    Boolean alive;
    Object syncObject = new Object();

    private void setAlive() {
        synchronized(syncObject) {
            alive = true;
            syncObject.notify();
        }
        System.out.println("Notifying application client has live connections");
    }

    public class Connector implements Runnable {
        public void run() {
            // get connected server list
            ArrayList<String> connectedServers = getConnectedServerList();
            boolean haveConnection = (connectedServers.size() > 0);

            // if no connections, try the initial server list
            if (!haveConnection) {
                haveConnection = connectToServers(initialServerList);

                // if connection succeeded, update the list
                if (haveConnection) {
                    connectedServers = getConnectedServerList();
                }
            }

            if (haveConnection) {
                // get list from the database of all live nodes in the cluster
                ArrayList<String> serversToConnect = getClusterMemberList();

                // subtract any nodes already connected
                serversToConnect.removeAll(connectedServers);

                // connect to the remaining nodes
                connectToServers(serversToConnect);
            }

            if (!alive && haveConnection) {
                // set connection alive status and notify any waiting threads
                setAlive();
            }

        }
    }

    class ConnectionStatusListener extends ClientStatusListenerExt {
        @Override
        public void connectionLost(
            String hostname,
            int port,
            int connectionsLeft,
            DisconnectCause cause) {
            if (alive) {
                System.err.printf("Connection to %s:%d was lost.\n", hostname, port);
            }
            // if no connections, set alive = false;
            if (getConnectedServerList().size() == 0) {
                alive = false;
            }
        }
    }

    public ConnectionManager(String servers, String username, String password) {

        // create client
        ClientConfig config = new ClientConfig(username,password,new ConnectionStatusListener());
        client = ClientFactory.createClient(config);
        alive = false;
        System.out.println("created client");

        // populate initial server list
        initialServerList.addAll(Arrays.asList(servers.split(",")));
        System.out.println("Set Initial Server List:");
        for (String s : initialServerList) {
            System.out.println("  " + s);
        }

        // start executor thread for getting and maintaining connections
        executor = Executors.newScheduledThreadPool(1);

        // schedule a Connector task to run immediately and then periodically
        int initialDelay = 0;
        int period = 10;
        executor.scheduleWithFixedDelay(new Connector(),initialDelay,period,TimeUnit.SECONDS);

    }

    public Client getClient() {

        // wait until alive (client has connections)
        try {
            synchronized(syncObject) {
                while(! alive) {
                    syncObject.wait();
                }
            }
        } catch (InterruptedException e) {
        }
        return client;
    }

    public ArrayList<String> getConnectedServerList() {
        ArrayList<String> list = new ArrayList<String>();
        List<InetSocketAddress> connectedHosts = client.getConnectedHostList();
        for (InetSocketAddress a : connectedHosts) {
            String hostname = a.getAddress().getHostName();
            String port = Integer.toString(a.getPort());
            String ip = getRealIP(a.getAddress().getHostAddress());
            String server = ip + ":" + port;

            if (!list.contains(server)) {
                list.add(server);
            }
        }
        return list;
    }

    public ArrayList<String> getClusterMemberList() {
        ArrayList<String> clusterMemberList = new ArrayList<String>();
        try {
            VoltTable overview = client.callProcedure("@SystemInformation","OVERVIEW").getResults()[0];
            String server = "";
            while (overview.advanceRow()) {
                String key = overview.getString(1);
                if (key.equals("IPADDRESS"))
                    server = overview.getString(2);
                if (key.equals("CLIENTPORT")) {
                    server = server + ":" + overview.getString(2);;
                    clusterMemberList.add(server);
                }
            }
        } catch (Exception e) {
        }
        return clusterMemberList;
    }

    // get the real ip, not 127.0.0.1
    public static String getRealIP(String ip) {
        if (ip.equals("127.0.0.1")) {
            try {
                Enumeration<NetworkInterface> n = NetworkInterface.getNetworkInterfaces();
                while (n.hasMoreElements()) {
                    NetworkInterface e = n.nextElement();
                    Enumeration<InetAddress> a = e.getInetAddresses();
                    while (a.hasMoreElements()) {
                        InetAddress addr = a.nextElement();
                        if (!addr.isLinkLocalAddress()
                            && !addr.isLoopbackAddress()
                            && addr instanceof Inet4Address) {
                            ip = addr.getHostAddress();
                        }
                    }
                }
            } catch (Exception e) {}
        }
        return ip;
    }

    // try to connect to each server in a list
    // return true if any connections were made successfully
    public boolean connectToServers(ArrayList<String> list) {
        boolean gotConnection = false;
        for (String server : list) {
            if (connect(server)) {
                gotConnection = true;
            }
        }
        return gotConnection;
    }

    // try to connect to a server
    // return true if successful
    public boolean connect(String server) {
            try {
                client.createConnection(server);
                System.out.println("Connected to " + server);
                return true;
            } catch (UnknownHostException uhe) {
                System.out.println("Host unknown: " + server);
                //uhe.printStackTrace();
                return false;
            } catch (IOException ioe) {
                System.out.println("Exception while connecting to " + server);
                System.out.println(ioe.getMessage());
                //ioe.printStackTrace();
                return false;
            }
    }

    public void close() {
        try {
            System.out.println("Shutting down ConnectionManager connector thread");
            executor.shutdown();
            alive = false;
            System.out.println("Closing client");
            client.drain();
            client.close();
        } catch (NoConnectionsException nce) {
            // that's fine, do nothing
        } catch (InterruptedException ie) {
            ie.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        String serverlist = "localhost";
        String username = "";
        String password = "";
        if (args.length > 0)
            serverlist = args[0];
        if (args.length > 1)
            username = args[1];
        if (args.length > 2)
            password = args[2];

        ConnectionManager manager = new ConnectionManager(serverlist,username,password);
        Client c = manager.getClient();
        System.out.println("Sleeping for 120 seconds");
        Thread.sleep(120000);
        System.out.println("Stopping client");
        manager.close();
    }

}
