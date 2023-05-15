package org.matsim.prepare;

import org.geotools.data.shapefile.files.ShpFiles;
import org.geotools.data.shapefile.shp.ShapefileReader;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.MultiPolygon;
import org.matsim.core.network.NetworkUtils;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Objects;

public class ImplementCityWideSpeedLimit {

    private static MultiPolygon readBerlinShape(GeometryFactory geometryFactory) throws IOException {
        var file = Paths.get("scenarios", "berlin-v5.5-10pct", "input", "berlin.dbf").toFile();
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
            boolean isContained = shape.contains(geometryFactory.createPoint(new Coordinate(x, y)));
            node.getAttributes().putAttribute("berlin", isContained);
        }

        for (var link : network.getLinks().values()){
            var fromNode = link.getFromNode().getAttributes().getAttribute("berlin");
            var toNode = link.getToNode().getAttributes().getAttribute("berlin");
            var type = link.getAttributes().getAttribute("type");
            if (((boolean) fromNode || (boolean) toNode) && !Objects.equals(type, "motorway")){
                link.setFreespeed(4.166666666666667);
            }
        }

        NetworkUtils.writeNetwork(network, Paths.get("scenarios", "berlin-v5.5-10pct", "input", "berlin-v5.5-network-annotated.xml.gz").toString());
    }

}
