package service;

import ch.hsr.geohash.BoundingBox;
import ch.hsr.geohash.GeoHash;
import filter.WithinFilter;
import model.QueryMatch;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class WithinQuery {

    static final byte[] TABLE = "wifi".getBytes();
    static final byte[] FAMILY = "a".getBytes();
    static final byte[] ID = "id".getBytes();
    static final byte[] X_COL = "lon".getBytes();
    static final byte[] Y_COL = "lat".getBytes();

    private static final String usage =
            "service.WithinQuery local|remote wkt\n" +
            "  help - print this message and exit.\n" +
            "  local | remote - run the exclusion filter client-side or in the filter.\n" +
            "  wkt - the query geometry in Well-Known Text format.";

    final GeometryFactory factory = new GeometryFactory();
    final Connection connection;

    public WithinQuery(Connection connection) {
        this.connection = connection;
    }

    Set<Coordinate> getCoords(GeoHash hash) {
        BoundingBox boundingBox = hash.getBoundingBox();
        Set<Coordinate> coordinates = new HashSet<Coordinate>(4);
        coordinates.add(new Coordinate(boundingBox.getMinLon(), boundingBox.getMinLat()));
        coordinates.add(new Coordinate(boundingBox.getMinLon(), boundingBox.getMaxLat()));
        coordinates.add(new Coordinate(boundingBox.getMaxLon(), boundingBox.getMinLat()));
        coordinates.add(new Coordinate(boundingBox.getMaxLon(), boundingBox.getMaxLat()));
        return coordinates;
    }

    Geometry convexHull(GeoHash[] hashes) {
        Set<Coordinate> coordinates = new HashSet<Coordinate>();
        for (GeoHash hash: hashes) {
            coordinates.addAll(getCoords(hash));
        }
        GeometryFactory factory = new GeometryFactory();
        Geometry geometry = factory.createMultiPoint(coordinates.toArray(new Coordinate[0]));
        return geometry.convexHull();
    }

    GeoHash[] minimumBoundingPrefixes(Geometry query) {
        GeoHash candidate;
        Geometry candidateGeom;
        Point queryCenter = query.getCentroid();
        for (int precision = 7; precision > 0; precision--) {
            candidate = GeoHash.withCharacterPrecision(queryCenter.getY(), queryCenter.getX(), precision);
            candidateGeom = convexHull(new GeoHash[]{ candidate });
            if (candidateGeom.contains(query)) {
                return new GeoHash[]{ candidate };
            }
            candidateGeom = convexHull(candidate.getAdjacent());
            if (candidateGeom.contains(query)) {
                GeoHash[] ret = Arrays.copyOf(candidate.getAdjacent(), 9);
                ret[8] = candidate;
                return ret;
            }
        }
        throw new IllegalArgumentException("Geometry cannot be contained by GeoHashs");
    }

    public Set<QueryMatch> query(Geometry query) throws IOException {
        GeoHash[] prefixes = minimumBoundingPrefixes(query);
        Set<QueryMatch> ret = new HashSet<>();
        Table table = connection.getTable(TableName.valueOf(TABLE));

        for (GeoHash prefix: prefixes) {
            byte[] p = prefix.toBase32().getBytes();
            Scan scan = new Scan().withStartRow(p);
            scan.setFilter(new PrefixFilter(p));
            scan.addFamily(FAMILY);
            scan.readVersions(1);
            scan.setCaching(50);

            ResultScanner scanner = table.getScanner(scan);
            for (Result result: scanner) {
                String hash = new String(result.getRow());
                String id = new String(result.getValue(FAMILY, ID));
                String lon = new String(result.getValue(FAMILY, X_COL));
                String lat = new String(result.getValue(FAMILY, Y_COL));
                ret.add(new QueryMatch(id, hash, Double.parseDouble(lon), Double.parseDouble(lat)));
            }
        }
        table.close();

        int exclusionCount = 0;
        for (Iterator<QueryMatch> iter = ret.iterator(); iter.hasNext();) {
            QueryMatch candidate = iter.next();
            Coordinate coordinate = new Coordinate(candidate.lon, candidate.lat);
            Geometry point = factory.createPoint(coordinate);
            if (!query.contains(point)) {
                iter.remove();
                exclusionCount++;
            }
        }
        System.out.println("Geometry predicate filtered " + exclusionCount + " points.");
        return ret;
    }

    public Set<QueryMatch> queryWithFilter(Geometry query) throws IOException {
        GeoHash[] prefixes = minimumBoundingPrefixes(query);
        Filter withinFilter = new WithinFilter(query);
        Set<QueryMatch> ret = new HashSet<>();
        Table table = connection.getTable(TableName.valueOf(TABLE));

        for (GeoHash prefix: prefixes) {
            byte[] p = prefix.toBase32().getBytes();
            FilterList filters = new FilterList(new PrefixFilter(p), withinFilter);
            Scan scan = new Scan().withStartRow(p).setFilter(filters);
            scan.addFamily(FAMILY);
            scan.readVersions(1);
            scan.setCaching(50);

            ResultScanner scanner = table.getScanner(scan);
            for (Result result: scanner) {
                String hash = new String(result.getRow());
                String id = new String(result.getValue(FAMILY, ID));
                String lon = new String(result.getValue(FAMILY, X_COL));
                String lat = new String(result.getValue(FAMILY, Y_COL));
                ret.add(new QueryMatch(id, hash, Double.parseDouble(lon), Double.parseDouble(lat)));
            }
        }
        table.close();
        return ret;
    }

    public static void main(String[] args) throws IOException, ParseException {
        if (args.length != 2 || (!"local".equals(args[0]) && !"remote".equals(args[0]))) {
            System.out.println(usage);
            System.exit(0);
        }

        WKTReader reader = new WKTReader();
        Geometry query = reader.read(args[1]);

        Connection connection = ConnectionFactory.createConnection();
        WithinQuery q = new WithinQuery(connection);
        Set<QueryMatch> results;
        if ("local".equals(args[0])) {
            results = q.query(query);
        } else {
            results = q.queryWithFilter(query);
        }

        System.out.println("Query matched " + results.size() + " points.");
        for (QueryMatch result: results) {
            System.out.println(result);
        }

        connection.close();
    }
}
