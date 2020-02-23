package service;

import ch.hsr.geohash.GeoHash;
import com.google.common.collect.MinMaxPriorityQueue;
import model.DistanceComparator;
import model.QueryMatch;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;

import java.io.IOException;
import java.util.Comparator;
import java.util.Queue;

public class KNNQuery {

    static final byte[] TABLE = "wifi".getBytes();
    static final byte[] FAMILY = "a".getBytes();
    static final byte[] ID = "id".getBytes();
    static final byte[] X_COL = "lon".getBytes();
    static final byte[] Y_COL = "lat".getBytes();

    private static final String usage =
        "service.KNNQuery lon lat n\n" +
        "   help - print this message and exit.\n" +
        "   lon, lat - query position.\n" +
        "   n - the number of neighbors to return.";
    final Connection connection;
    int precision = 7;

    public KNNQuery(Connection connection) {
        this.connection = connection;
    }

    public KNNQuery(Connection connection, int characterPrecision) {
        this.connection = connection;
        this.precision = characterPrecision;
    }

    Queue<QueryMatch> takeN(Comparator<QueryMatch> comparator, String prefix, int n) throws IOException {
        Queue<QueryMatch> candidates = MinMaxPriorityQueue.orderedBy(comparator).maximumSize(n).create();
        Scan scan = new Scan().withStartRow(prefix.getBytes());
        scan.setFilter(new PrefixFilter(prefix.getBytes()));
        scan.addFamily(FAMILY);
        scan.readVersions(1);
        scan.setCaching(50);

        Table table = connection.getTable(TableName.valueOf(TABLE));

        int cnt = 0;
        ResultScanner scanner = table.getScanner(scan);
        for (Result result: scanner) {
            String hash = new String(result.getRow());
            String id = new String(result.getValue(FAMILY, ID));
            double lon = Double.parseDouble(new String(result.getValue(FAMILY, X_COL)));
            double lat = Double.parseDouble(new String(result.getValue(FAMILY, Y_COL)));
            QueryMatch q = new QueryMatch(id, hash, lon, lat);
            q.distance = ((DistanceComparator)comparator).calculateDistance(q);
            candidates.add(q);
            cnt++;
        }
        table.close();
        System.out.println(String.format("Scan over '%s' returned %s candidates.", prefix, cnt));
        return candidates;
    }

    public Queue<QueryMatch> queryKNN(double lon, double lat, int n) throws IOException {
        DistanceComparator comparator = new DistanceComparator(lon, lat);
        Queue<QueryMatch> ret = MinMaxPriorityQueue.orderedBy(comparator).maximumSize(n).create();
        GeoHash target;
        for (int pre = precision; pre != 1; pre--) {
            target = GeoHash.withCharacterPrecision(lat, lon, pre);
            ret.addAll(takeN(comparator, target.toBase32(), n));
            for (GeoHash hash: target.getAdjacent()) {
                ret.addAll(takeN(comparator, hash.toBase32(), n));
            }
            if (ret.size() == n) {
                break;
            } else {
                ret.clear();
            }
        }
        return ret;
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.out.println(usage);
            System.exit(0);
        }

        double lon = Double.parseDouble(args[0]);
        double lat = Double.parseDouble(args[1]);
        int n = Integer.parseInt(args[2]);

        Connection connection = ConnectionFactory.createConnection();
        KNNQuery query = new KNNQuery(connection);
        Queue<QueryMatch> ret = query.queryKNN(lon, lat, n);

        QueryMatch m;
        while ((m = ret.poll()) != null) {
            System.out.println(m);
        }

        connection.close();
    }
}
