package org.matsim.prepare;

import org.apache.log4j.Logger;
import org.geotools.data.shapefile.files.ShpFiles;
import org.geotools.data.shapefile.shp.ShapefileReader;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.MultiPolygon;
import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.network.Network;
import org.matsim.api.core.v01.network.Node;
import org.matsim.core.config.Config;
import org.matsim.core.config.ConfigUtils;
import org.matsim.core.network.NetworkUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class ScenarioCreator {

    /**
     * Information for each node whether it is in Berlin
     */
    private final Map<Id<Node>, Boolean> isInBerlinByNode;
    /**
     * Path to the original config
     */
    private final Path originalConfigPath;
    /**
     * Read original config
     */
    private final Config originalConfig;
    /**
     * Read original network
     */
    private final Network originalNetwork;
    /**
     * Path to the scenarios folder
     */
    private final Path scenariosPath;

    private final static Logger log = Logger.getLogger(ScenarioCreator.class);

    public enum RoadKind {

        MAIN_STREET,
        SIDE_STREET,
        OTHER_STREET;

        private static final Set<String> ROAD_TYPES_MAIN_STREETS = Set.of("primary", "primary_link", "secondary", "secondary_link", "tertiary");
        private static final Set<String> ROAD_TYPES_SIDE_STREETS = Set.of("residential", "living_street", "unclassified");

        public static RoadKind fromValue(String value) {
            if (value == null) {
                return OTHER_STREET;
            } else if (ROAD_TYPES_MAIN_STREETS.contains(value)) {
                return MAIN_STREET;
            } else if (ROAD_TYPES_SIDE_STREETS.contains(value)) {
                return SIDE_STREET;
            } else {
                return OTHER_STREET;
            }
        }


    }

    public static class LinkData {

        /**
         * Link of the network
         */
        private final Link link;
        /**
         * Whether the link is in the subnetwork, meaning it is accessible by car and in Berlin
         */
        private final boolean matchesSubNetwork;
        /**
         * What kind of road this link represents
         */
        private final RoadKind roadKind;

        public LinkData(Link link, Map<Id<Node>, Boolean> isInBerlinByNode) {
            this.link = link;
            var isFromNodeInBerlin = isInBerlinByNode.get(link.getFromNode().getId());
            var isToNodeInBerlin = isInBerlinByNode.get(link.getToNode().getId());
            boolean isInBerlin = isFromNodeInBerlin || isToNodeInBerlin;
            this.matchesSubNetwork = isInBerlin && !link.getAllowedModes().contains("pt");
            var attributes = link.getAttributes();
            var type = (String) attributes.getAttribute("type");
            this.roadKind = RoadKind.fromValue(type);
            attributes.putAttribute("matchesSubNetwork", matchesSubNetwork);
            attributes.putAttribute("roadKind", roadKind.name().toLowerCase());
            attributes.putAttribute("originalFreespeed", link.getFreespeed());
            attributes.putAttribute("originalFlowCapacity", link.getCapacity());
            attributes.putAttribute("originalNumberOfLanes", link.getNumberOfLanes());
        }

    }

    public ScenarioCreator(GeometryFactory geometryFactory, MultiPolygon berlinShape, Path configPath) {
        this.originalConfigPath = configPath;
        this.originalConfig = ConfigUtils.loadConfig(configPath.toString());
        Path scenarioPath = configPath.getParent();
        this.originalNetwork = NetworkUtils.readNetwork(scenarioPath.resolve("berlin-v5.5-network.xml.gz").toString());
        this.isInBerlinByNode = inspectNodes(originalNetwork, geometryFactory, berlinShape);
        this.scenariosPath = scenarioPath.getParent().getParent();
    }

    /**
     * Inspects which nodes are in Berlin
     *
     * @return Map which describes for each node whether it is in Berlin
     */
    private static Map<Id<Node>, Boolean> inspectNodes(Network network, GeometryFactory geometryFactory,
                                                       MultiPolygon berlinShape) {
        Map<Id<Node>, Boolean> isInBerlinByNode = new HashMap<>();
        for (var node : network.getNodes().values()) {
            double x = node.getCoord().getX();
            double y = node.getCoord().getY();
            boolean isContained = berlinShape.contains(geometryFactory.createPoint(new Coordinate(x, y)));
            node.getAttributes().putAttribute("isInBerlin", isContained);
            isInBerlinByNode.put(node.getId(), isContained);
        }
        return isInBerlinByNode;
    }

    /**
     * Inspects links respective whether they are in Berlin, they are in the matching subnetwork and their road kind
     *
     * @return Links with additional data
     */
    private List<LinkData> inspectLinks(Network network, Map<Id<Node>, Boolean> isInBerlinByNode) {
        return network.getLinks().values().stream()
                .map(link -> new LinkData(link, isInBerlinByNode))
                .collect(Collectors.toList());
    }

    private Config createConfigClone() {
        return ConfigUtils.loadConfig(originalConfig.getContext());
    }

    /**
     * Clones the original network so its links can be manipulated independently.
     * This is much faster than rereading through I/O.
     * The nodes are reused as those are not modified.
     *
     * @return Network with cloned links
     */
    private Network createNetworkClone() {
        Network network = NetworkUtils.createNetwork(originalConfig);
        for (Node node : originalNetwork.getNodes().values()) {
            network.addNode(node);
        }
        for (Link originalLink : originalNetwork.getLinks().values()) {
            Link link = network.getFactory().createLink(originalLink.getId(), originalLink.getFromNode(), originalLink.getToNode());
            link.setLength(originalLink.getLength());
            link.setFreespeed(originalLink.getFreespeed());
            link.setCapacity(originalLink.getCapacity());
            link.setAllowedModes(originalLink.getAllowedModes());
            link.setNumberOfLanes(originalLink.getNumberOfLanes());
            for (Map.Entry<String, Object> entry : originalLink.getAttributes().getAsMap().entrySet()) {
                link.getAttributes().putAttribute(entry.getKey(), entry.getValue());
            }
            network.addLink(link);
        }
        return network;
    }

    /**
     * Creates a scenario and saves its files into a folder
     *
     * @param abbreviation Unique abbreviation for the scenario
     * @param modifiers    Modifiers for the network
     */
    public void createScenario(String abbreviation, List<Consumer<LinkData>> modifiers) {
        log.info(String.format("Creating scenario %s", abbreviation));
        Path scenarioPath = scenariosPath.resolve(String.format("berlin-v5.5-%s", abbreviation)).resolve("input");
        boolean scenarioDirectoryCreated = scenarioPath.toFile().mkdirs();
        if (scenarioDirectoryCreated) {
            log.info(String.format("Directory %s created", scenarioPath));
        }
        Path createdNetworkPath = scenarioPath.resolve(String.format("berlin-v5.5.network-%s.xml.gz", abbreviation));
        NetworkUtils.writeNetwork(createModifiedNetwork(modifiers), createdNetworkPath.toString());
        Path createdConfigPath = scenarioPath.resolve(String.format("berlin-v5.5.config-%s.xml", abbreviation));
        ConfigUtils.writeConfig(createModifiedConfig(createdNetworkPath), createdConfigPath.toString());
    }

    /**
     * Creates a modified config for a scenario
     *
     * @return modified config which is suitable for the scenario
     */
    private Config createModifiedConfig(Path createdNetworkPath) {
        Path relativePath = createdNetworkPath.getParent().relativize(originalConfigPath.getParent());
        Config config = createConfigClone();
        config.controler().setLastIteration(100);
        config.controler().setOutputDirectory(Paths.get("..", "output").toString());
        config.network().setInputFile(createdNetworkPath.getFileName().toString());
        config.plans().setInputFile(relativePath.resolve(originalConfig.plans().getInputFile()).toString());
        config.transit().setTransitScheduleFile(relativePath.resolve(originalConfig.transit().getTransitScheduleFile()).toString());
        config.transit().setVehiclesFile(relativePath.resolve(originalConfig.transit().getVehiclesFile()).toString());
        config.vehicles().setVehiclesFile(relativePath.resolve(originalConfig.vehicles().getVehiclesFile()).toString());
        return config;
    }

    /**
     * Creates a modified network
     *
     * @return Freshly created network with the modifiers applied
     */
    private Network createModifiedNetwork(List<Consumer<LinkData>> modifiers) {
        Network network = createNetworkClone();
        var linkDatas = inspectLinks(network, isInBerlinByNode);
        for (Consumer<LinkData> modifier : modifiers) {
            for (LinkData linkData : linkDatas) {
                modifier.accept(linkData);
            }
        }
        return network;
    }

    public static void writeModifiedAttribute(Link link, String abbreviation, boolean isModified) {
        link.getAttributes().putAttribute(String.format("isModifiedBy-%s", abbreviation), isModified);
    }

    public static void reduceFreespeedOnMainStreets(LinkData linkData) {
        double speedLimit = 30 / 7.2;
        boolean isModified = linkData.matchesSubNetwork
                && linkData.link.getFreespeed() > speedLimit
                && linkData.roadKind == RoadKind.MAIN_STREET;
        if (isModified) {
            linkData.link.setFreespeed(speedLimit);
        }
        writeModifiedAttribute(linkData.link, "GR-HS", isModified);
    }

    public static void reduceFreespeedOnSideStreets(LinkData linkData) {
        double speedLimit = 15 / 7.2;
        boolean isModified = linkData.matchesSubNetwork
                && linkData.link.getFreespeed() > speedLimit
                && linkData.roadKind == RoadKind.SIDE_STREET;
        if (isModified) {
            linkData.link.setFreespeed(speedLimit);
        }
        writeModifiedAttribute(linkData.link, "GR-WS", isModified);
    }

    public static void reduceCapacityOnMainStreets(LinkData linkData) {
        double numberOfLanes = linkData.link.getNumberOfLanes();
        boolean isModified = linkData.matchesSubNetwork
                && linkData.roadKind == RoadKind.MAIN_STREET;
        if (isModified) {
            if (numberOfLanes <= 1) {
                linkData.link.setCapacity(0.5 * linkData.link.getCapacity());
            } else if (numberOfLanes < 2) {
                linkData.link.setCapacity(0.5 * linkData.link.getCapacity());
                linkData.link.setNumberOfLanes(1);
            } else {
                linkData.link.setCapacity((numberOfLanes - 1) / numberOfLanes * linkData.link.getCapacity());
                linkData.link.setNumberOfLanes(numberOfLanes - 1);
            }
        }
        writeModifiedAttribute(linkData.link, "KR-HS", isModified);
    }

    public static void reduceCapacityOnSideStreets(LinkData linkData) {
        boolean isModified = linkData.matchesSubNetwork
                && linkData.roadKind == RoadKind.SIDE_STREET;
        if (isModified) {
            linkData.link.setCapacity(0.5 * linkData.link.getCapacity());
        }
        writeModifiedAttribute(linkData.link, "KR-WS", isModified);
    }

    public static void kietzblocks(LinkData linkData) {
        boolean isModified = linkData.matchesSubNetwork
                && linkData.roadKind == RoadKind.SIDE_STREET;
        if (isModified) {
            linkData.link.setCapacity(0.1);
        }
        writeModifiedAttribute(linkData.link, "KB", isModified);
    }

    public static void carBan(LinkData linkData) {
        boolean isModified = linkData.matchesSubNetwork
                && linkData.roadKind != RoadKind.OTHER_STREET;
        if (isModified) {
            linkData.link.setCapacity(0.1);
        }
        writeModifiedAttribute(linkData.link, "MV", isModified);
    }

    private static MultiPolygon readBerlinShape(GeometryFactory geometryFactory) throws IOException {
        var file = Paths.get("scenarios", "berlin-v5.5-10pct", "input", "berlin-shp", "berlin.dbf").toFile();
        var reader = new ShapefileReader(new ShpFiles(file), true, true, geometryFactory);
        var shape = (MultiPolygon) reader.nextRecord().shape();
        reader.close();
        return shape;
    }

    public static void main(String[] args) throws IOException {
        GeometryFactory geometryFactory = new GeometryFactory();
        MultiPolygon berlinShape = readBerlinShape(geometryFactory);

        Path networkPath = Paths.get("scenarios", "berlin-v5.5-10pct", "input", "berlin-v5.5-10pct.config.xml");
        var scenarioCreator = new ScenarioCreator(geometryFactory, berlinShape, networkPath);
        // create base scenario (a new network file will be created because they have additional attributes)
        scenarioCreator.createScenario("BASE", List.of());
        // create scenarios with a single measure
        scenarioCreator.createScenario("GR-HS", List.of(ScenarioCreator::reduceFreespeedOnMainStreets));
        scenarioCreator.createScenario("GR-WS", List.of(ScenarioCreator::reduceFreespeedOnSideStreets));
        scenarioCreator.createScenario("KR-HS", List.of(ScenarioCreator::reduceCapacityOnMainStreets));
        scenarioCreator.createScenario("KR-WS", List.of(ScenarioCreator::reduceCapacityOnSideStreets));
        scenarioCreator.createScenario("KB", List.of(ScenarioCreator::kietzblocks));
        scenarioCreator.createScenario("MV", List.of(ScenarioCreator::carBan));
        // create stacked scenarios
        scenarioCreator.createScenario("S1", List.of(
                ScenarioCreator::reduceFreespeedOnMainStreets,
                ScenarioCreator::reduceFreespeedOnSideStreets));
        scenarioCreator.createScenario("S2", List.of(
                ScenarioCreator::reduceFreespeedOnMainStreets,
                ScenarioCreator::reduceFreespeedOnSideStreets,
                ScenarioCreator::reduceCapacityOnMainStreets));
        scenarioCreator.createScenario("S3", List.of(
                ScenarioCreator::reduceFreespeedOnMainStreets,
                ScenarioCreator::reduceFreespeedOnSideStreets,
                ScenarioCreator::reduceCapacityOnMainStreets,
                ScenarioCreator::reduceCapacityOnSideStreets));
        scenarioCreator.createScenario("S4", List.of(
                ScenarioCreator::reduceFreespeedOnMainStreets,
                ScenarioCreator::reduceFreespeedOnSideStreets,
                ScenarioCreator::reduceCapacityOnMainStreets,
                ScenarioCreator::reduceCapacityOnSideStreets,
                ScenarioCreator::kietzblocks));
    }

}
