package service;

import ch.hsr.geohash.GeoHash;
import com.google.common.base.Splitter;
import org.apache.commons.collections.iterators.ArrayIterator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Ingest {

    private static final String usage =
            "ingest table source.csv\n" +
            "  help - print this message and exit.\n" +
            "  table - the target table to load.\n" +
            "  source.csv - path to the csv file to load.\n" +
            "\n" +
            "load data from source.csv. assumes new-line delimited, comma-separated\n" +
            "records. drops the first line. generates a geohash for the rowkey.\n" +
            "records are stored in columns in the 'a' family, columns are:\n" +
            "  lon,lat,id\\n";

    private static final byte[] FAMILY = "a".getBytes();
    private static final String[] COLUMNS = new String[] {
            "lon", "lat", "id"
    };
    private static final ArrayIterator COLS = new ArrayIterator(COLUMNS);
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().limit(COLUMNS.length);

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.out.println(usage);
            System.exit(0);
        }

        Connection connection = ConnectionFactory.createConnection();
        BufferedMutator bufferedMutator = connection.getBufferedMutator(TableName.valueOf(args[0]));
        bufferedMutator.disableWriteBufferPeriodicFlush();

        BufferedReader reader = new BufferedReader(new FileReader(args[1]));
        String line = reader.readLine();
        int records = 0;
        long start = System.currentTimeMillis();

        while ((line = reader.readLine()) != null) {
            COLS.reset();
            Iterator<String> vals = SPLITTER.split(line).iterator();
            Map<String, String> row = new HashMap<>(COLUMNS.length);

            while (vals.hasNext() && COLS.hasNext()) {
                String col = (String) COLS.next();
                String val = vals.next();
                row.put(col, val);
            }

            double lat = Double.parseDouble(row.get("lat"));
            double lon = Double.parseDouble(row.get("lon"));
            String rowkey = GeoHash.withCharacterPrecision(lat, lon, 12).toBase32();
            Put put = new Put(rowkey.getBytes());
            for (Map.Entry<String, String> entry: row.entrySet()) {
                put.addColumn(FAMILY, entry.getKey().getBytes(), entry.getValue().getBytes());
            }

            bufferedMutator.mutate(put);
            records++;
        }

        bufferedMutator.flush();
        long end = System.currentTimeMillis();
        System.out.println(String.format("Geohashed %s records in %sms.", records, end - start));

        reader.close();
        bufferedMutator.close();
        connection.close();
    }

}
