package benchmark;

import java.util.Random;
import org.voltdb.*;
import org.voltdb.client.*;

public class Benchmark {

    private Client client;
    private Random rand = new Random();
    private BenchmarkStats stats;

    public Benchmark(String servers) throws Exception {
        client = ClientFactory.createClient();
        String[] serverArray = servers.split(",");
        for (String server : serverArray) {
            client.createConnection(server);
        }

        stats = new BenchmarkStats(client);
    }


    public void init() throws Exception {

        // any initial setup can go here

    }


    public void runBenchmark() throws Exception {

        stats.startBenchmark();

        for (int i=0; i<1000000; i++) {

            int appid = rand.nextInt(50);
            int deviceid = rand.nextInt(1000000);
            int sessions = (appid + deviceid) % 5;

            for (int j=0; j<sessions; j++) {
                client.callProcedure(new BenchmarkCallback("APP_SESSION.insert"),
                                     "APP_SESSION.insert",
                                     appid,
                                     deviceid,
                                     null
                                     );
            }

        }

        stats.endBenchmark();

        client.drain();
        BenchmarkCallback.printAllResults();

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
