package org.matsim.prepare;

import org.geotools.data.shapefile.files.ShpFiles;
import org.geotools.data.shapefile.shp.ShapefileReader;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.MultiPolygon;
import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.network.Node;
import org.matsim.core.network.NetworkUtils;
import org.matsim.utils.objectattributes.attributable.Attributes;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ImplementCityWideSpeedLimit {

    private static final Set<String> ROAD_TYPES_EXCLUDED_FROM_SPEED_LIMIT = Set.of("motorway", "motorway_link", "trunk", "trunk_link");

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
        Map<Id<Node>, Boolean> isInBerlinByNode = new HashMap<>();
        for (var node : network.getNodes().values()) {
            double x = node.getCoord().getX();
            double y = node.getCoord().getY();
            boolean isContained = shape.contains(geometryFactory.createPoint(new Coordinate(x, y)));
            node.getAttributes().putAttribute("isInBerlin", isContained);
            isInBerlinByNode.put(node.getId(), isContained);
        }

        double speedLimit = 30 / 7.2;

        // reduziert für alle bisher schnelleren links in Berlin außer den exkludierten den freespeed auf unser speedLimit
        for (var link : network.getLinks().values()) {
            var isFromNodeInBerlin = isInBerlinByNode.get(link.getFromNode().getId());
            var isToNodeInBerlin = isInBerlinByNode.get(link.getToNode().getId());
            Attributes attributes = link.getAttributes();
            var type = (String) attributes.getAttribute("type");
            var modes = link.getAllowedModes();
            boolean isInBerlin = isFromNodeInBerlin || isToNodeInBerlin;
            boolean isRelevantRoadType = type != null && !ROAD_TYPES_EXCLUDED_FROM_SPEED_LIMIT.contains(type);
            boolean isRelevantMode = !modes.contains("pt");
            boolean isFasterThanSpeedLimit = link.getFreespeed() > speedLimit;
            attributes.putAttribute("isInBerlin", isInBerlin);
            attributes.putAttribute("isRelevantRoadType", isRelevantRoadType);
            attributes.putAttribute("isRelevantMode", isRelevantMode);
            attributes.putAttribute("isFasterThanSpeedLimit", isFasterThanSpeedLimit);
            if (isInBerlin && isRelevantRoadType && isRelevantMode && isFasterThanSpeedLimit) {
                link.setFreespeed(speedLimit);
                attributes.putAttribute("isModified", true);
            } else {
                attributes.putAttribute("isModified", false);
            }
        }

        NetworkUtils.writeNetwork(network, Paths.get("scenarios", "berlin-v5.5-10pct", "input", "berlin-v5.5-network-with-speed-limit.xml.gz").toString());
    }

}
