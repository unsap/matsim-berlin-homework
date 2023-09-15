package org.matsim.analysis;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.network.Network;
import org.matsim.api.core.v01.population.Activity;
import org.matsim.api.core.v01.population.Leg;
import org.matsim.api.core.v01.population.Person;
import org.matsim.api.core.v01.population.Plan;
import org.matsim.api.core.v01.population.PlanElement;
import org.matsim.common.BerlinScenario;
import org.matsim.core.config.Config;
import org.matsim.core.config.ConfigUtils;
import org.matsim.core.network.NetworkUtils;
import org.matsim.core.population.algorithms.PersonAlgorithm;
import org.matsim.core.population.io.StreamingPopulationReader;
import org.matsim.core.population.routes.NetworkRoute;
import org.matsim.core.router.TripStructureUtils;
import org.matsim.core.scenario.ScenarioUtils;
import org.matsim.prepare.ScenarioCreator.AreaKind;

public class TripBerlinwiseAnalysis {

    private final static Logger log = Logger.getLogger(TripBerlinwiseAnalysis.class);

    private static class TripData {

        private final Id<Person> personId;
        private final int tripNumber;
        private final List<String> modes = new ArrayList<>();

        private final Id<Link> startLinkId;
        private final AreaKind startAreaKind;
        private final Set<AreaKind> visitedAreaKinds = new HashSet<>();
        private Id<Link> lastLinkId;
        private AreaKind lastAreaKind;

        private TripData(Id<Person> personId, int tripNumber, Id<Link> startLinkId, AreaKind startAreaKind) {
            this.personId = personId;
            this.tripNumber = tripNumber;
            this.startLinkId = startLinkId;
            this.startAreaKind = startAreaKind;
            this.visitedAreaKinds.add(startAreaKind);
            this.lastLinkId = startLinkId;
            this.lastAreaKind = startAreaKind;
        }

        private void complete(Id<Link> lastLinkId, AreaKind lastAreaKind) {
            this.lastLinkId = lastLinkId;
            this.lastAreaKind = lastAreaKind;
        }

        public String getCsvRow() {
            String visitedAreaKindsValue = visitedAreaKinds.stream().map(
                    areaKind -> areaKind.name().toLowerCase()).collect(Collectors.joining(","));
            return String.format("%s;%s;%s_%s;%s;%s;%s;%s;%s;%s", personId, tripNumber, personId, tripNumber,
                    String.join("-", modes), startLinkId, lastLinkId, startAreaKind.name().toLowerCase(),
                    visitedAreaKindsValue, lastAreaKind.name().toLowerCase());
        }

    }

    private static class TripBerlinwisePlanPersonAlgorithm implements PersonAlgorithm {

        private final Map<Id<Link>, AreaKind> areaKindByLink;
        private final List<TripData> trips = new ArrayList<>();

        private TripBerlinwisePlanPersonAlgorithm(Map<Id<Link>, AreaKind> areaKindByLink) {
            this.areaKindByLink = areaKindByLink;
        }

        @Override
        public void run(Person person) {
            Plan selectedPlan = person.getSelectedPlan();
            int tripNumber = 1;
            TripData currentTrip = null;
            for (PlanElement planElement : selectedPlan.getPlanElements()) {
                if (planElement instanceof Activity) {
                    Activity activity = (Activity) planElement;
                    if (!TripStructureUtils.isStageActivityType(activity.getType())) {
                        if (currentTrip != null) {
                            Id<Link> linkId = activity.getLinkId();
                            AreaKind areaKind = areaKindByLink.get(linkId);
                            currentTrip.complete(linkId, areaKind);
                            currentTrip = null;
                        }
                    }
                } else {
                    Leg leg = (Leg) planElement;
                    if (currentTrip == null) {
                        Id<Link> startLinkId = leg.getRoute().getStartLinkId();
                        AreaKind startArea = areaKindByLink.get(startLinkId);
                        currentTrip = new TripData(person.getId(), tripNumber, startLinkId, startArea);
                        trips.add(currentTrip);
                        tripNumber += 1;
                    }
                    currentTrip.modes.add(leg.getMode());
                    if (leg.getRoute() instanceof NetworkRoute) {
                        NetworkRoute networkRoute = (NetworkRoute) leg.getRoute();
                        for (Id<Link> linkId : networkRoute.getLinkIds()) {
                            AreaKind areaKind = areaKindByLink.get(linkId);
                            currentTrip.visitedAreaKinds.add(areaKind);
                        }
                    }
                }
            }
        }

        public void writeCsv(Writer writer) throws IOException {
            writer.write(
                    "person;trip_number;trip_id;modes;start_link;end_link;start_area;visited_areas;end_area" + System.lineSeparator());
            for (TripData trip : trips) {
                writer.write(trip.getCsvRow() + System.lineSeparator());
            }
        }

    }

    public static Map<Id<Link>, AreaKind> getAreaKindByLink(Network network) {
        return network.getLinks().values().stream()
                .collect(Collectors.toMap(Link::getId, link -> {
                    String areaKindValue = (String) link.getAttributes().getAttribute("areaKind");
                    return AreaKind.valueOf(areaKindValue.toUpperCase());
                }));
    }

    public static void runAnalysis(Path scenariosPath, BerlinScenario scenario) throws IOException {
        Path scenarioPath = scenariosPath.resolve(scenario.getDirectoryName());
        Path inputPath = scenarioPath.resolve("input");
        Path outputPath = scenarioPath.resolve("output");

        Path networkPath = inputPath.resolve(String.format("%s.network.xml.gz", scenario.getFilePrefix()));
        Network network = NetworkUtils.readNetwork(networkPath.toString());
        Map<Id<Link>, AreaKind> areaKindByLink = getAreaKindByLink(network);

        Path plansPath = outputPath.resolve(String.format("%s.output_plans.xml.gz", scenario.getFilePrefix()));
        Config config = ConfigUtils.createConfig();
        StreamingPopulationReader streamingPopulationReader = new StreamingPopulationReader(
                ScenarioUtils.createScenario(config));
        TripBerlinwisePlanPersonAlgorithm personAlgorithm = new TripBerlinwisePlanPersonAlgorithm(areaKindByLink);
        streamingPopulationReader.addAlgorithm(personAlgorithm);
        streamingPopulationReader.readFile(plansPath.toString());

        Path analysisPath = scenarioPath.resolve("analysis");
        boolean analysisDirectoryCreated = analysisPath.toFile().mkdirs();
        if (analysisDirectoryCreated) {
            log.info(String.format("Directory %s created", scenarioPath));
        }
        Path csvPath = analysisPath.resolve(String.format("%s.trips_berlinwise.csv", scenario.getFilePrefix()));
        try (Writer writer = new FileWriter(csvPath.toFile())) {
            personAlgorithm.writeCsv(writer);
        }
    }

    /**
     * Run this with:
     * 1. The path to the scenarios directory
     * 2. Either the name of a single scenario you want to check, or "all" for all scenarios
     */
    public static void main(String[] args) throws IOException {
        if (args[1].equals("all")) {
            for (BerlinScenario scenario : BerlinScenario.values()) {
                System.out.printf("Running on %s%n", scenario.getDirectoryName());
                runAnalysis(Path.of(args[0]), scenario);
            }
        } else {
            runAnalysis(Path.of(args[0]), BerlinScenario.valueOf(args[1]));
        }
    }

}
