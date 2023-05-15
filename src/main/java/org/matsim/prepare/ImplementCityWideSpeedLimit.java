package org.matsim.prepare;

import org.geotools.data.shapefile.files.ShpFiles;
import org.geotools.data.shapefile.shp.ShapefileReader;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.MultiPolygon;
import org.matsim.core.network.NetworkUtils;

import java.io.IOException;
import java.nio.file.Paths;

public class ImplementCityWideSpeedLimit {

    private static MultiPolygon readBerlinShape(GeometryFactory geometryFactory) throws IOException {
        var file = Paths.get("scenarios", "berlin-v5.5-10pct", "input", "berlin-shp", "berlin.dbf").toFile();
        var reader = new ShapefileReader(new ShpFiles(file), true, true, geometryFactory);
        var shape = (MultiPolygon) reader.nextRecord().shape();
        reader.close();
        return shape;
    }

    public static void main(String[] args) throws IOException {
        GeometryFactory geometryFactory = new GeometryFactory();
        var shape = readBerlinShape(geometryFactory);
        var network = NetworkUtils.readNetwork(Paths.get("scenarios", "berlin-v5.5-10pct", "input", "berlin-v5.5-network.xml.gz").toString());
        for (var node : network.getNodes().values()) {
            double x = node.getCoord().getX();
            double y = node.getCoord().getY();
            var isContained = shape.contains(geometryFactory.createPoint(new Coordinate(x, y)));
            node.getAttributes().putAttribute("berlin", isContained);
        }
        NetworkUtils.writeNetwork(network, Paths.get("scenarios", "berlin-v5.5-10pct", "input", "berlin-v5.5-network-annotated.xml.gz").toString());
    }

}
