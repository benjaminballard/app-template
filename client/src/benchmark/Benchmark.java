package benchmark;

import java.util.Random;
import org.voltdb.*;
import org.voltdb.client.*;
import org.voltdb.client.VoltBulkLoader.*;
import org.voltdb.types.*;

public class Benchmark {

    private Client client;
    private Random rand = new Random();
    private VoltBulkLoader bulkLoader;
    private int testSize = 1000000;
    private int bulkLoaderBatchSize = 500;

    public Benchmark(String servers) throws Exception {
        client = ClientFactory.createClient();
        String[] serverArray = servers.split(",");
        for (String server : serverArray) {
            client.createConnection(server);
        }

        // Get a BulkLoader for the table we want to load, with a given batch size and one callback handles failures for any failed batches
        bulkLoader = client.getNewBulkLoader("app_session",bulkLoaderBatchSize, new SessionFailureCallback());
    }

    // we must implement the BulkLoaderFailureCallBack interface, there is no default implementation
    public class SessionFailureCallback implements BulkLoaderFailureCallBack {
        @Override
        public void failureCallback(Object rowHandle, Object[] fieldList, ClientResponse response) {
            // do nothing for now
            // for a fuller example, see: https://github.com/VoltDB/voltdb/blob/master/src/frontend/org/voltdb/utils/CSVBulkDataLoader.java#L57
        }
    }


    // The failure callback provides an Object "rowHandle", which can be any object
    // The purpose is to handle the error by logging the bad row, or resubmitting the row, or whatever
    // It can either be an object that encapsulates the existing row data, or something like a POJO
    // In this example, the row data is stored as attributes, but also provided as an Object[] which simplifies the call to insertRow()
    public class Session {
        public int appid;
        public int deviceid;
        public TimestampType time;
        Object[] objectArray = {appid, deviceid, time};

        public Session(int appid, int deviceid, TimestampType time) {
            this.appid=appid;
            this.deviceid=deviceid;
            this.time=time;
        }

        public Object[] toArray() {
            return objectArray;
        }
    }


    public void init() throws Exception {
        // any initial setup (e.g. loading data) before the benchmark can go here
    }


    public void runBenchmark() throws Exception {

        // For comparison, here is typical benchmark data generation using procedure calls
        System.out.println("Benchmarking procedure calls...");
        long startNanos = System.nanoTime();
        for (int i=0; i<testSize; i++) {

            int appid = rand.nextInt(50);
            int deviceid = rand.nextInt(1000000);
            TimestampType time = new TimestampType();

            client.callProcedure(new BenchmarkCallback("APP_SESSION.insert"),
                                 "APP_SESSION.insert",
                                 appid,
                                 deviceid,
                                 time
                                 );
        }
        double elapsedSeconds = (System.nanoTime() - startNanos)/1000000000.0;
        int tps = (int)(testSize/elapsedSeconds);
        System.out.println("Loaded "+testSize+" records in "+elapsedSeconds+" seconds ("+tps+" rows/sec)");


        // Here is benchmark data generation using the BulkLoader
        System.out.println("Benchmarking with VoltBulkLoader...");
        startNanos = System.nanoTime();
        for (int i=0; i<testSize; i++) {

            int appid = rand.nextInt(50);
            int deviceid = rand.nextInt(1000000);
            TimestampType time = new TimestampType();
            Session s = new Session(appid,deviceid,time);

            bulkLoader.insertRow(s, s.toArray());
        }
        bulkLoader.drain();
        client.drain();
        elapsedSeconds = (System.nanoTime() - startNanos)/1000000000.0;
        tps = (int)(testSize/elapsedSeconds);
        System.out.println("Loaded "+testSize+" records in "+elapsedSeconds+" seconds ("+tps+" rows/sec)");


        bulkLoader.close();
        client.close();
    }


    public static void main(String[] args) throws Exception {

        String serverlist = "localhost";
        if (args.length > 0) { serverlist = args[0]; }
        Benchmark benchmark = new Benchmark(serverlist);
        benchmark.init();
        benchmark.runBenchmark();

    }
}
