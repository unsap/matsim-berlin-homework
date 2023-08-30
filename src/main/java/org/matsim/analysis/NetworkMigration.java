package org.matsim.analysis;

import org.apache.log4j.Logger;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.MultiPolygon;
import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.network.Network;
import org.matsim.api.core.v01.network.Node;
import org.matsim.common.BerlinScenario;
import org.matsim.core.network.NetworkUtils;
import org.matsim.prepare.ScenarioCreator;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import static org.matsim.prepare.ScenarioCreator.*;

public class NetworkMigration {

    private final static Logger log = Logger.getLogger(NetworkMigration.class);

    public static void migrateAll(Path scenariosPath) throws IOException, FactoryException, TransformException {
        GeometryFactory geometryFactory = new GeometryFactory();
        Path inputPath = scenariosPath.resolve("berlin-v5.5-10pct").resolve("input");
        MultiPolygon berlinShape = readShape(geometryFactory, inputPath.resolve("berlin-shp").resolve("berlin.dbf"));
        MultiPolygon berlinUmweltzoneShape = transformShape(readShape(geometryFactory, inputPath.resolve("berlin-shp").resolve("Umweltzone_Berlin.dbf")), "EPSG:25833");

        Path originalNetworkPath = inputPath.resolve("berlin-v5.5-network.xml.gz");
        Network originalNetwork = NetworkUtils.readNetwork(originalNetworkPath.toString());
        Map<Id<Node>, ScenarioCreator.AreaKind> areaKindByNode = inspectNodes(originalNetwork, geometryFactory, berlinShape, berlinUmweltzoneShape);

        for (BerlinScenario scenario : BerlinScenario.values()) {
            migrate(areaKindByNode, scenariosPath, scenario.getScenarioName());
        }
    }

    public static void migrate(Map<Id<Node>, AreaKind> areaKindByNode, Path scenariosPath, String scenario) {
        log.info(String.format("Migrating %s", scenario));
        migrateNetwork(areaKindByNode, scenariosPath.resolve(scenario).resolve("input").resolve(String.format("%s.network.xml.gz", scenario)));
        migrateNetwork(areaKindByNode, scenariosPath.resolve(scenario).resolve("output").resolve(String.format("%s.output_network.xml.gz", scenario)));
    }

    private static void migrateNetwork(Map<Id<Node>, ScenarioCreator.AreaKind> areaKindByNode, Path networkPath) {
        Network network = NetworkUtils.readNetwork(networkPath.toString());
        for (Node node : network.getNodes().values()) {
            migrateNode(areaKindByNode, node);
        }
        for (Link link : network.getLinks().values()) {
            migrateLink(areaKindByNode, link);
        }
        NetworkUtils.writeNetwork(network, networkPath.toString());
    }

    private static void migrateNode(Map<Id<Node>, AreaKind> areaKindByNode, Node node) {
        var areaKind = areaKindByNode.get(node.getId());
        var attributes = node.getAttributes();
        attributes.removeAttribute("isInBerlin");
        attributes.putAttribute("areaKind", areaKind.name().toLowerCase());
    }

    private static void migrateLink(Map<Id<Node>, ScenarioCreator.AreaKind> areaKindByNode, Link link) {
        boolean isAccessibleByCar = link.getAllowedModes().contains("car");
        var areaKindOfFromNode = areaKindByNode.get(link.getFromNode().getId());
        var areaKindOfToNode = areaKindByNode.get(link.getToNode().getId());
        ScenarioCreator.AreaKind areaKind;
        if (areaKindOfFromNode == areaKindOfToNode) {
            areaKind = areaKindOfFromNode;
        } else {
            // At least one node is in Berlin, but not both are in the Umweltzone.
            // The best fit for this link is Berlin outside the Umweltzone.
            areaKind = ScenarioCreator.AreaKind.BERLIN_OUTSIDE_UMWELTZONE;
        }
        var attributes = link.getAttributes();
        attributes.removeAttribute("matchesSubNetwork");
        attributes.putAttribute("isAccessibleByCar", isAccessibleByCar);
        attributes.putAttribute("areaKind", areaKind.name().toLowerCase());
    }

    /**
     * Run this with:
     * 1. The path to the scenarios directory
     */
    public static void main(String[] args) throws IOException, FactoryException, TransformException {
        migrateAll(Path.of(args[0]));
    }

}
