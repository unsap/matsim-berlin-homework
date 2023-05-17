package org.matsim.prepare;

import org.geotools.data.shapefile.files.ShpFiles;
import org.geotools.data.shapefile.shp.ShapefileReader;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.MultiPolygon;
import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.network.Node;
import org.matsim.core.network.NetworkUtils;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ImplementCityWideSpeedLimit {

    private static MultiPolygon readBerlinShape(GeometryFactory geometryFactory) throws IOException {
        var file = Paths.get("scenarios", "berlin-v5.5-10pct", "input", "berlin-shp", "berlin.dbf").toFile();
        var reader = new ShapefileReader(new ShpFiles(file), true, true, geometryFactory);
        var shape = (MultiPolygon) reader.nextRecord().shape();
        reader.close();
        return shape;
    }

    public static void main(String[] args) throws IOException {

        // builds bool HashMap der network-nodes ob in Berlin
        GeometryFactory geometryFactory = new GeometryFactory();
        var shape = readBerlinShape(geometryFactory);
        var network = NetworkUtils.readNetwork(Paths.get("scenarios", "berlin-v5.5-10pct", "input", "berlin-v5.5-network.xml.gz").toString());
        Map<Id<Node>, Boolean> isBerlinByNode = new HashMap<>();
        for (var node : network.getNodes().values()) {
            double x = node.getCoord().getX();
            double y = node.getCoord().getY();
            boolean isContained = shape.contains(geometryFactory.createPoint(new Coordinate(x, y)));
            node.getAttributes().putAttribute("isBerlin", isContained);
            isBerlinByNode.put(node.getId(), isContained);
        }

        double speedLimit = 15 / 3.6;

        // reduziert für alle bisher schnelleren links in Berlin außer motorways den freespeed auf unser speedLimit
        for (var link : network.getLinks().values()) {
            var fromNode = isBerlinByNode.get(link.getFromNode().getId());
            var toNode = isBerlinByNode.get(link.getToNode().getId());
            var type = link.getAttributes().getAttribute("type");
            if ((fromNode || toNode) && !Objects.equals(type, "motorway") && link.getFreespeed() > speedLimit) {
                link.setFreespeed(speedLimit);
                link.getAttributes().putAttribute("isModified", true);
            } else {
                link.getAttributes().putAttribute("isModified", false);
            }
        }

        NetworkUtils.writeNetwork(network, Paths.get("scenarios", "berlin-v5.5-10pct", "input", "berlin-v5.5-network-annotated.xml.gz").toString());
    }

}
