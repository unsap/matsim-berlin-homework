package org.matsim.prepare;

import org.apache.log4j.Logger;
import org.geotools.data.shapefile.files.ShpFiles;
import org.geotools.data.shapefile.shp.ShapefileReader;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.network.Network;
import org.matsim.api.core.v01.network.Node;
import org.matsim.common.BerlinScenario;
import org.matsim.core.config.Config;
import org.matsim.core.config.ConfigUtils;
import org.matsim.core.network.NetworkUtils;
import org.matsim.core.utils.geometry.geotools.MGC;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

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
    private final Map<Id<Node>, AreaKind> areaKindByNode;
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

    public enum AreaKind {

        BRANDENBURG,
        BERLIN_OUTSIDE_UMWELTZONE,
        BERLIN_UMWELTZONE;

        public boolean isInBerlin() {
            return this == BERLIN_OUTSIDE_UMWELTZONE || this == BERLIN_UMWELTZONE;
        }

    }

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
         * Whether the link is accessible by car
         */
        private final boolean isAccessibleByCar;
        /**
         * To which area this link belongs
         */
        private final AreaKind areaKind;
        /**
         * What kind of road this link represents
         */
        private final RoadKind roadKind;

        public LinkData(Link link, Map<Id<Node>, AreaKind> areaKindByNode) {
            this.link = link;
            this.isAccessibleByCar = link.getAllowedModes().contains("car");
            var areaKindOfFromNode = areaKindByNode.get(link.getFromNode().getId());
            var areaKindOfToNode = areaKindByNode.get(link.getToNode().getId());
            if (areaKindOfFromNode == areaKindOfToNode) {
                this.areaKind = areaKindOfFromNode;
            } else {
                // At least one node is in Berlin, but not both are in the Umweltzone.
                // The best fit for this link is Berlin outside the Umweltzone.
                this.areaKind = AreaKind.BERLIN_OUTSIDE_UMWELTZONE;
            }
            var attributes = link.getAttributes();
            var type = (String) attributes.getAttribute("type");
            this.roadKind = RoadKind.fromValue(type);
            attributes.putAttribute("isAccessibleByCar", isAccessibleByCar);
            attributes.putAttribute("areaKind", areaKind.name().toLowerCase());
            attributes.putAttribute("roadKind", roadKind.name().toLowerCase());
            attributes.putAttribute("originalFreespeed", link.getFreespeed());
            attributes.putAttribute("originalFlowCapacity", link.getCapacity());
            attributes.putAttribute("originalNumberOfLanes", link.getNumberOfLanes());
        }

    }

    public ScenarioCreator(GeometryFactory geometryFactory, MultiPolygon berlinShape, MultiPolygon berlinUmweltzoneShape, Path configPath) {
        this.originalConfigPath = configPath;
        this.originalConfig = ConfigUtils.loadConfig(configPath.toString());
        Path scenarioPath = configPath.getParent();
        this.originalNetwork = NetworkUtils.readNetwork(scenarioPath.resolve("berlin-v5.5-network.xml.gz").toString());
        this.areaKindByNode = inspectNodes(originalNetwork, geometryFactory, berlinShape, berlinUmweltzoneShape);
        this.scenariosPath = scenarioPath.getParent().getParent();
    }

    /**
     * Inspects which nodes are in Berlin
     *
     * @return Map which describes for each node whether it is in Berlin
     */
    public static Map<Id<Node>, AreaKind> inspectNodes(Network network, GeometryFactory geometryFactory,
                                                       MultiPolygon berlinShape,
                                                       MultiPolygon berlinUmweltzoneShape) {
        Map<Id<Node>, AreaKind> areaKindByNode = new HashMap<>();
        for (var node : network.getNodes().values()) {
            double x = node.getCoord().getX();
            double y = node.getCoord().getY();
            Point point = geometryFactory.createPoint(new Coordinate(x, y));
            AreaKind areaKind;
            if (!berlinShape.contains(point)) {
                areaKind = AreaKind.BRANDENBURG;
            } else if (!berlinUmweltzoneShape.contains(point)) {
                areaKind = AreaKind.BERLIN_OUTSIDE_UMWELTZONE;
            } else {
                areaKind = AreaKind.BERLIN_UMWELTZONE;
            }
            node.getAttributes().putAttribute("areaKind", areaKind.name().toLowerCase());
            areaKindByNode.put(node.getId(), areaKind);
        }
        return areaKindByNode;
    }

    /**
     * Inspects links respective whether they are in Berlin, they are in the matching subnetwork and their road kind
     *
     * @return Links with additional data
     */
    private List<LinkData> inspectLinks(Network network) {
        return network.getLinks().values().stream()
                .map(link -> new LinkData(link, areaKindByNode))
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
     * @param scenario  Unique name for the scenario
     * @param modifiers Modifiers for the network
     */
    public void createScenario(String scenario, List<Consumer<LinkData>> modifiers) {
        log.info(String.format("Creating scenario %s", scenario));
        Path scenarioPath = scenariosPath.resolve(scenario).resolve("input");
        boolean scenarioDirectoryCreated = scenarioPath.toFile().mkdirs();
        if (scenarioDirectoryCreated) {
            log.info(String.format("Directory %s created", scenarioPath));
        }
        Path createdNetworkPath = scenarioPath.resolve(String.format("%s.network.xml.gz", scenario));
        NetworkUtils.writeNetwork(createModifiedNetwork(modifiers), createdNetworkPath.toString());
        Path createdConfigPath = scenarioPath.resolve(String.format("%s.config.xml", scenario));
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
        config.controler().setOutputDirectory(Paths.get("output").toString());
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
        var linkDatas = inspectLinks(network);
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
        boolean isModified = linkData.isAccessibleByCar
                && linkData.areaKind.isInBerlin()
                && linkData.link.getFreespeed() > speedLimit
                && linkData.roadKind == RoadKind.MAIN_STREET;
        if (isModified) {
            linkData.link.setFreespeed(speedLimit);
        }
        writeModifiedAttribute(linkData.link, "GR-HS", isModified);
    }

    public static void reduceFreespeedOnSideStreets(LinkData linkData) {
        double speedLimit = 15 / 7.2;
        boolean isModified = linkData.isAccessibleByCar
                && linkData.areaKind.isInBerlin()
                && linkData.link.getFreespeed() > speedLimit
                && linkData.roadKind == RoadKind.SIDE_STREET;
        if (isModified) {
            linkData.link.setFreespeed(speedLimit);
        }
        writeModifiedAttribute(linkData.link, "GR-WS", isModified);
    }

    public static void reduceCapacityOnMainStreets(LinkData linkData) {
        double numberOfLanes = linkData.link.getNumberOfLanes();
        boolean isModified = linkData.isAccessibleByCar
                && linkData.areaKind.isInBerlin()
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

    public static void kiezblocksOnlyInUmweltzone(LinkData linkData) {
        boolean isModified = linkData.isAccessibleByCar
                && linkData.areaKind == AreaKind.BERLIN_UMWELTZONE
                && linkData.roadKind == RoadKind.SIDE_STREET;
        if (isModified) {
            linkData.link.setCapacity(0.1);
        }
        writeModifiedAttribute(linkData.link, "KB-2", isModified);
    }

    public static void carBan(LinkData linkData) {
        boolean isModified = linkData.isAccessibleByCar
                && linkData.areaKind.isInBerlin()
                && linkData.roadKind != RoadKind.OTHER_STREET;
        if (isModified) {
            linkData.link.setCapacity(0.1);
        }
        writeModifiedAttribute(linkData.link, "MV", isModified);
    }

    public static MultiPolygon readShape(GeometryFactory geometryFactory, Path shapeFilePath) throws IOException {
        var file = shapeFilePath.toFile();
        var reader = new ShapefileReader(new ShpFiles(file), true, true, geometryFactory);
        var shape = (MultiPolygon) reader.nextRecord().shape();
        reader.close();
        return shape;
    }

    public static MultiPolygon transformShape(MultiPolygon shape, String shapeCRS) throws FactoryException, TransformException {
        CoordinateReferenceSystem sourceCRS = MGC.getCRS(shapeCRS);
        CoordinateReferenceSystem targetCRS = MGC.getCRS("EPSG:31468");
        MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS, true);
        return (MultiPolygon) JTS.transform(shape, transform);
    }

    public static void main(String[] args) throws IOException, FactoryException, TransformException {
        GeometryFactory geometryFactory = new GeometryFactory();
        Path inputPath = Paths.get("scenarios", "berlin-v5.5-10pct", "input");
        Path configPath = inputPath.resolve("berlin-v5.5-10pct.config.xml");
        MultiPolygon berlinShape = readShape(geometryFactory, inputPath.resolve("berlin-shp").resolve("berlin.dbf"));
        MultiPolygon berlinUmweltzoneShape = transformShape(readShape(geometryFactory, inputPath.resolve("berlin-shp").resolve("Umweltzone_Berlin.dbf")), "EPSG:25833");

        var scenarioCreator = new ScenarioCreator(geometryFactory, berlinShape, berlinUmweltzoneShape, configPath);
        // create base scenario (a new network file will be created because they have additional attributes)
        scenarioCreator.createScenario(BerlinScenario.BASE.getScenarioName(), List.of());
        // create scenarios with a single measure
        scenarioCreator.createScenario(BerlinScenario.GR_HS.getScenarioName(), List.of(ScenarioCreator::reduceFreespeedOnMainStreets));
        scenarioCreator.createScenario(BerlinScenario.GR_WS.getScenarioName(), List.of(ScenarioCreator::reduceFreespeedOnSideStreets));
        scenarioCreator.createScenario(BerlinScenario.KR_HS.getScenarioName(), List.of(ScenarioCreator::reduceCapacityOnMainStreets));
        scenarioCreator.createScenario(BerlinScenario.KB.getScenarioName(), List.of(ScenarioCreator::kiezblocksOnlyInUmweltzone));
        scenarioCreator.createScenario(BerlinScenario.MV.getScenarioName(), List.of(ScenarioCreator::carBan));
        // create stacked scenarios
        scenarioCreator.createScenario(BerlinScenario.S1.getScenarioName(), List.of(
                ScenarioCreator::reduceFreespeedOnMainStreets,
                ScenarioCreator::reduceFreespeedOnSideStreets));
        scenarioCreator.createScenario(BerlinScenario.S2.getScenarioName(), List.of(
                ScenarioCreator::reduceFreespeedOnMainStreets,
                ScenarioCreator::reduceFreespeedOnSideStreets,
                ScenarioCreator::reduceCapacityOnMainStreets));
        scenarioCreator.createScenario(BerlinScenario.S3.getScenarioName(), List.of(
                ScenarioCreator::reduceFreespeedOnMainStreets,
                ScenarioCreator::reduceFreespeedOnSideStreets,
                ScenarioCreator::reduceCapacityOnMainStreets,
                ScenarioCreator::kiezblocksOnlyInUmweltzone));
    }

}
