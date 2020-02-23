package filter;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.io.IOException;
import java.util.List;

public class WithinFilter extends FilterBase {

    static final byte[] TABLE = "wifi".getBytes();
    static final byte[] FAMILY = "a".getBytes();
    static final byte[] ID = "id".getBytes();
    static final byte[] X_COL = "lon".getBytes();
    static final byte[] Y_COL = "lat".getBytes();

    static final Log LOG = LogFactory.getLog(WithinFilter.class);

    final GeometryFactory factory = new GeometryFactory();
    static final GeometryFactory sfactory = new GeometryFactory();
    Geometry query = null;
    boolean exclude = false;

    public WithinFilter() {}

    public WithinFilter(Geometry query) {
        this.query = query;
    }

    @Override
    public boolean hasFilterRow() {
        return true;
    }

    @Override
    public void filterRowCells(List<Cell> cells) throws IOException {
        double lon = Double.NaN;
        double lat = Double.NaN;
        if (null == cells || 0 == cells.size()) {
            LOG.debug("skipping empty row.");
            this.exclude = true;
            return;
        }

        for (Cell cell: cells) {
            byte[] qual = cell.getQualifierArray();
            if (Bytes.equals(qual, X_COL)) {
                lon = Double.parseDouble(new String(cell.getValueArray()));
            }
            if (Bytes.equals(qual, Y_COL)) {
                lat = Double.parseDouble(new String(cell.getValueArray()));
            }
        }

        if (Double.isNaN(lat) || Double.isNaN(lon)) {
            LOG.debug(new String(cells.get(0).getRowArray()) + " is not a point.");
            this.exclude = true;
            return;
        }

        Coordinate coordinate = new Coordinate(lon, lat);
        Geometry point = factory.createPoint(coordinate);
        if (!query.contains(point)) {
            this.exclude = true;
        }
    }

    @Override
    public boolean filterRow() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("filter applied. " + (this.exclude ? "rejecting" : "keeping"));
        }
        return this.exclude;
    }

    @Override
    public void reset() {
        this.exclude = false;
    }

    @Override
    public byte[] toByteArray() throws IOException {
        WithinFilterProtos.WithinFilter.Builder builder = WithinFilterProtos.WithinFilter.newBuilder();
        if (query!=null) {
            builder.setQuery(ByteStringer.wrap(Bytes.toBytes(query.toText())));
        }        return builder.build().toByteArray();
    }

    public static Filter parseFrom(final byte[] pbBytes) throws DeserializationException {
        WithinFilterProtos.WithinFilter proto = null;
        Geometry geometry = null;
        WKTReader reader = new WKTReader(sfactory);
        try {
            proto = WithinFilterProtos.WithinFilter.parseFrom(pbBytes);
            String wkt = Bytes.toString(proto.getQuery().toByteArray());
            geometry = reader.read(wkt);
        } catch (InvalidProtocolBufferException | ParseException e) {
            throw new DeserializationException(e);
        }
        return new WithinFilter(geometry);
    }

}
