package org.matsim.analysis;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.events.ActivityEndEvent;
import org.matsim.api.core.v01.events.ActivityStartEvent;
import org.matsim.api.core.v01.events.LinkEnterEvent;
import org.matsim.api.core.v01.events.PersonDepartureEvent;
import org.matsim.api.core.v01.events.PersonEntersVehicleEvent;
import org.matsim.api.core.v01.events.PersonLeavesVehicleEvent;
import org.matsim.api.core.v01.events.handler.ActivityEndEventHandler;
import org.matsim.api.core.v01.events.handler.ActivityStartEventHandler;
import org.matsim.api.core.v01.events.handler.LinkEnterEventHandler;
import org.matsim.api.core.v01.events.handler.PersonDepartureEventHandler;
import org.matsim.api.core.v01.events.handler.PersonEntersVehicleEventHandler;
import org.matsim.api.core.v01.events.handler.PersonLeavesVehicleEventHandler;
import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.network.Network;
import org.matsim.api.core.v01.population.Person;
import org.matsim.common.BerlinScenario;
import org.matsim.core.api.experimental.events.EventsManager;
import org.matsim.core.events.EventsUtils;
import org.matsim.core.network.NetworkUtils;
import org.matsim.core.router.TripStructureUtils;
import org.matsim.prepare.ScenarioCreator.AreaKind;
import org.matsim.prepare.ScenarioCreator.RoadKind;
import org.matsim.vehicles.Vehicle;

import com.google.common.collect.Iterables;

public class TripEventsAnalysis {

    private final static Logger log = Logger.getLogger(TripEventsAnalysis.class);

    private enum DistanceCategory {
        CAR_BERLIN_A_MAIN_STREET,
        CAR_BERLIN_A_SIDE_STREET,
        CAR_BERLIN_A_OTHER_STREET,
        CAR_BERLIN_B_MAIN_STREET,
        CAR_BERLIN_B_SIDE_STREET,
        CAR_BERLIN_B_OTHER_STREET,
        CAR_BRANDENBURG,
        PT_BERLIN_A,
        PT_BERLIN_B,
        PT_BRANDENBURG;

        public static DistanceCategory fromModeAndLink(String mode, Link link) {
            switch (mode) {
                case "car": {
                    switch (getAreaKind(link)) {
                        case BERLIN_UMWELTZONE:
                            switch (getRoadKind(link)) {
                                case MAIN_STREET:
                                    return CAR_BERLIN_A_MAIN_STREET;
                                case SIDE_STREET:
                                    return CAR_BERLIN_A_SIDE_STREET;
                                case OTHER_STREET:
                                    return CAR_BERLIN_A_OTHER_STREET;
                            }
                        case BERLIN_OUTSIDE_UMWELTZONE:
                            switch (getRoadKind(link)) {
                                case MAIN_STREET:
                                    return CAR_BERLIN_B_MAIN_STREET;
                                case SIDE_STREET:
                                    return CAR_BERLIN_B_SIDE_STREET;
                                case OTHER_STREET:
                                    return CAR_BERLIN_B_OTHER_STREET;
                            }
                        case BRANDENBURG:
                            return CAR_BRANDENBURG;
                    }
                }
                case "pt": {
                    switch (getAreaKind(link)) {
                        case BERLIN_UMWELTZONE:
                            return PT_BERLIN_A;
                        case BERLIN_OUTSIDE_UMWELTZONE:
                            return PT_BERLIN_B;
                        case BRANDENBURG:
                            return PT_BRANDENBURG;
                    }
                }
                default:
                    return null;
            }
        }

    }

    private static class TripData {

        private final Id<Person> personId;
        private final int tripNumber;
        private String currentMode;
        private final List<String> modes = new ArrayList<>();

        private final Map<DistanceCategory, Double> distanceByCategory = new EnumMap<>(DistanceCategory.class);

        private int visitedLinks = 0;
        private Id<Link> lastLinkId;
        private AreaKind lastAreaKind;
        /**
         * A trip is completed after an activity has started which does not belong to the interaction activities
         */
        private boolean completed = false;

        private TripData(Id<Person> personId, int tripNumber) {
            this.personId = personId;
            this.tripNumber = tripNumber;
        }

        public void setCurrentMode(String currentMode) {
            this.currentMode = currentMode;
            modes.add(currentMode);
        }

        private void visit(Link link) {
            lastLinkId = link.getId();
            lastAreaKind = getAreaKind(link);
            visitedLinks += 1;
            DistanceCategory distanceCategory = DistanceCategory.fromModeAndLink(currentMode, link);
            if (distanceCategory != null) {
                distanceByCategory.compute(distanceCategory,
                        (a, distance) -> link.getLength() + Objects.requireNonNullElse(distance, 0.0));
            }
        }

        private void complete(Link lastLink) {
            lastLinkId = lastLink.getId();
            lastAreaKind = getAreaKind(lastLink);
            completed = true;
        }

        public String getCsvRow() {
            String distances = Arrays.stream(DistanceCategory.values())
                    .map(distanceCategory -> distanceByCategory.getOrDefault(distanceCategory, 0.0))
                    .map(distance -> String.format("%.1f", distance))
                    .collect(Collectors.joining(";"));
            return String.format("%s;%s;%s_%s;%s;%s;%s;%s;%d;%s", personId, tripNumber, personId,
                    tripNumber, String.join("-", modes), lastLinkId, lastAreaKind.name().toLowerCase(), completed,
                    visitedLinks, distances);
        }

    }

    private static class EventHandler implements
            ActivityEndEventHandler, PersonDepartureEventHandler, PersonEntersVehicleEventHandler, LinkEnterEventHandler, PersonLeavesVehicleEventHandler, ActivityStartEventHandler {

        private final Map<Id<Link>, ? extends Link> links;
        private final Map<Id<Vehicle>, Map<Id<Person>, TripData>> tripsByVehiclesInTraffic = new HashMap<>();
        private final Map<Id<Person>, List<TripData>> tripsByPerson = new HashMap<>();

        private EventHandler(Map<Id<Link>, ? extends Link> links) {
            this.links = links;
        }

        private static Optional<TripData> getIncompleteTrip(List<TripData> tripsOfPerson) {
            if (tripsOfPerson.isEmpty()) {
                return Optional.empty();
            }
            TripData lastTrip = Iterables.getLast(tripsOfPerson);
            if (lastTrip.completed) {
                return Optional.empty();
            } else {
                return Optional.of(lastTrip);
            }
        }

        @Override
        public void handleEvent(ActivityEndEvent event) {
            Id<Person> personId = event.getPersonId();
            Id<Link> linkId = event.getLinkId();
            List<TripData> tripsOfPerson = tripsByPerson.computeIfAbsent(personId, p -> new ArrayList<>());
            Optional<TripData> incompleteTrip = getIncompleteTrip(tripsOfPerson);
            if (incompleteTrip.isEmpty()) {
                int tripNumber = tripsOfPerson.size() + 1;
                TripData newTrip = new TripData(personId, tripNumber);
                tripsOfPerson.add(newTrip);
            }
        }

        @Override
        public void handleEvent(PersonDepartureEvent event) {
            Id<Person> personId = event.getPersonId();
            Id<Link> linkId = event.getLinkId();
            List<TripData> tripsOfPerson = tripsByPerson.computeIfAbsent(personId, p -> new ArrayList<>());
            Optional<TripData> incompleteTrip = getIncompleteTrip(tripsOfPerson);
            if (incompleteTrip.isPresent()) {
                incompleteTrip.get().setCurrentMode(event.getLegMode());
                incompleteTrip.get().visit(links.get(linkId));
            }
        }

        @Override
        public void handleEvent(PersonEntersVehicleEvent event) {
            Id<Person> personId = event.getPersonId();
            Id<Vehicle> vehicleId = event.getVehicleId();
            List<TripData> tripsOfPerson = tripsByPerson.computeIfAbsent(personId, p -> new ArrayList<>());
            Map<Id<Person>, TripData> tripsOfVehicle = tripsByVehiclesInTraffic.computeIfAbsent(vehicleId,
                    v -> new HashMap<>());
            Optional<TripData> trip = getIncompleteTrip(tripsOfPerson);
            trip.ifPresent(tripData -> tripsOfVehicle.put(personId, tripData));
        }

        @Override
        public void handleEvent(LinkEnterEvent event) {
            Id<Link> linkId = event.getLinkId();
            Link link = links.get(linkId);
            for (TripData tripData : tripsByVehiclesInTraffic.get(event.getVehicleId()).values()) {
                tripData.visit(link);
            }
        }

        @Override
        public void handleEvent(PersonLeavesVehicleEvent event) {
            tripsByVehiclesInTraffic.get(event.getVehicleId()).remove(event.getPersonId());
        }

        @Override
        public void handleEvent(ActivityStartEvent event) {
            if (!TripStructureUtils.isStageActivityType(event.getActType())) {
                List<TripData> tripsOfPerson = tripsByPerson.get(event.getPersonId());
                if (tripsOfPerson != null) {
                    Id<Link> linkId = event.getLinkId();
                    TripData lastTripOfPerson = Iterables.getLast(tripsOfPerson);
                    lastTripOfPerson.complete(links.get(linkId));
                }
            }
        }

        public void writeCsv(Writer writer) throws IOException {
            String distanceColumns = Arrays.stream(DistanceCategory.values())
                    .map(distanceCategory -> distanceCategory.name().toLowerCase())
                    .collect(Collectors.joining(";"));
            writer.write(String.format(
                    "person;trip_number;trip_id;modes;last_link;last_area;completed;visited_link_count;%s%n",
                    distanceColumns));
            Iterable<TripData> trips = tripsByPerson.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .flatMap(entry -> entry.getValue().stream())
                    ::iterator;
            for (TripData trip : trips) {
                writer.write(trip.getCsvRow() + System.lineSeparator());
            }
        }

    }

    public static RoadKind getRoadKind(Link link) {
        String roadKindValue = (String) link.getAttributes().getAttribute("roadKind");
        return RoadKind.valueOf(roadKindValue.toUpperCase());
    }

    public static AreaKind getAreaKind(Link link) {
        String areaKindValue = (String) link.getAttributes().getAttribute("areaKind");
        return AreaKind.valueOf(areaKindValue.toUpperCase());
    }

    public static void runAnalysis(Path scenariosPath, BerlinScenario scenario) throws IOException {
        Path scenarioPath = scenariosPath.resolve(scenario.getDirectoryName());
        Path inputPath = scenarioPath.resolve("input");
        Path outputPath = scenarioPath.resolve("output");

        Path networkPath = inputPath.resolve(String.format("%s.network.xml.gz", scenario.getFilePrefix()));
        Network network = NetworkUtils.readNetwork(networkPath.toString());

        Path eventsPath = outputPath.resolve(String.format("%s.output_events.xml.gz", scenario.getFilePrefix()));
        EventHandler eventHandler = new EventHandler(network.getLinks());
        EventsManager eventsManager = EventsUtils.createEventsManager();
        eventsManager.addHandler(eventHandler);

        EventsUtils.readEvents(eventsManager, eventsPath.toString());
        Path analysisPath = scenarioPath.resolve("analysis");
        boolean analysisDirectoryCreated = analysisPath.toFile().mkdirs();
        if (analysisDirectoryCreated) {
            log.info(String.format("Directory %s created", scenarioPath));
        }
        Path csvPath = analysisPath.resolve(String.format("%s.trips_additional.csv", scenario.getFilePrefix()));
        try (Writer writer = new FileWriter(csvPath.toFile())) {
            eventHandler.writeCsv(writer);
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
