package org.matsim.analysis;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.matsim.vehicles.Vehicle;

import com.google.common.collect.Iterables;

public class TripBerlinwiseAnalysis {

    private final static Logger log = Logger.getLogger(TripBerlinwiseAnalysis.class);

    private enum TrafficKind {

        BERLIN_ORIGIN("berlin_orig"),
        BERLIN_DESTINATION("berlin_dest"),
        BERLIN_INTERNAL("berlin_inner"),
        BERLIN_TRANSIT("berlin_transit"),
        NON_BERLIN("non_berlin");

        private final String value;

        TrafficKind(String value) {
            this.value = value;
        }

        public static TrafficKind fromValues(Boolean originInBerlin, boolean atLeastOneLinkInBerlin, Boolean destinationInBerlin) {
            if (originInBerlin == null || destinationInBerlin == null) {
                return null;
            } else if (originInBerlin) {
                if (destinationInBerlin) {
                    return BERLIN_INTERNAL;
                } else {
                    return BERLIN_ORIGIN;
                }
            } else {
                if (destinationInBerlin) {
                    return BERLIN_DESTINATION;
                } else if (atLeastOneLinkInBerlin) {
                    return BERLIN_TRANSIT;
                } else {
                    return NON_BERLIN;
                }
            }
        }

    }

    private static class TripData {

        private final Id<Person> personId;
        private final int tripNumber;
        private final List<String> modes = new ArrayList<>();

        private final Id<Link> startLinkId;
        private final boolean startLinkInBerlin;
        private boolean atLeastOneLinkInBerlin;
        private Id<Link> lastLinkId;
        private boolean lastLinkInBerlin;
        /**
         * A trip is completed after an activity has started which does not belong to the interaction activities
         */
        private boolean completed = false;

        private TripData(Id<Person> personId, int tripNumber, Id<Link> startLinkId, boolean startLinkInBerlin) {
            this.personId = personId;
            this.tripNumber = tripNumber;
            this.startLinkId = startLinkId;
            this.startLinkInBerlin = startLinkInBerlin;
            this.atLeastOneLinkInBerlin = startLinkInBerlin;
            this.lastLinkId = startLinkId;
            this.lastLinkInBerlin = startLinkInBerlin;
        }

        private void complete(Id<Link> lastLinkId, boolean lastLinkInBerlin) {
            this.lastLinkId = lastLinkId;
            this.lastLinkInBerlin = lastLinkInBerlin;
            this.completed = true;
        }

        public String getCsvRow() {
            TrafficKind trafficKind = TrafficKind.fromValues(startLinkInBerlin, atLeastOneLinkInBerlin,
                    lastLinkInBerlin);
            String trafficKindValue;
            if (trafficKind == null) {
                trafficKindValue = null;
            } else {
                trafficKindValue = trafficKind.value;
            }
            return String.format("%s;%s;%s_%s;%s;%s;%s;%s", personId, tripNumber, personId, tripNumber,
                    String.join("-", modes), startLinkId, lastLinkId, trafficKindValue);
        }

    }

    private static class EventHandler implements
            ActivityEndEventHandler, PersonDepartureEventHandler, PersonEntersVehicleEventHandler, LinkEnterEventHandler, PersonLeavesVehicleEventHandler, ActivityStartEventHandler {

        private final Map<Id<Link>, AreaKind> areaKindByLink;
        private final Map<Id<Vehicle>, Map<Id<Person>, TripData>> tripsByVehiclesInTraffic = new HashMap<>();
        private final Map<Id<Person>, List<TripData>> tripsByPerson = new HashMap<>();

        private EventHandler(Map<Id<Link>, AreaKind> areaKindByLink) {
            this.areaKindByLink = areaKindByLink;
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
                boolean startLinkInBerlin = areaKindByLink.get(linkId).isInBerlin();
                TripData newTrip = new TripData(personId, tripNumber, linkId, startLinkInBerlin);
                tripsOfPerson.add(newTrip);
            }
        }

        @Override
        public void handleEvent(PersonDepartureEvent event) {
            List<TripData> tripsOfPerson = tripsByPerson.get(event.getPersonId());
            if (tripsOfPerson != null) {
                TripData trip = Iterables.getLast(tripsOfPerson);
                trip.modes.add(event.getLegMode());
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
            AreaKind areaKind = areaKindByLink.get(linkId);
            for (TripData tripData : tripsByVehiclesInTraffic.get(event.getVehicleId()).values()) {
                tripData.atLeastOneLinkInBerlin |= areaKind.isInBerlin();
                tripData.lastLinkId = linkId;
                tripData.lastLinkInBerlin = areaKind.isInBerlin();
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
                    AreaKind areaKind = areaKindByLink.get(linkId);
                    TripData lastTripOfPerson = Iterables.getLast(tripsOfPerson);
                    lastTripOfPerson.complete(linkId, areaKind.isInBerlin());
                }
            }
        }

        public void writeCsv(Writer writer) throws IOException {
            writer.write("person;trip_number;trip_id;modes;start_link;end_link;berlinwise" + System.lineSeparator());
            Iterable<TripData> trips = tripsByPerson.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .flatMap(entry -> entry.getValue().stream())
                    ::iterator;
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

    public static void runAnalysis(Path scenariosPath, String scenario) throws IOException {
        Path scenarioPath = scenariosPath.resolve(scenario);
        Path inputPath = scenarioPath.resolve("input");
        Path outputPath = scenarioPath.resolve("output");

        Path networkPath = inputPath.resolve(String.format("%s.network.xml.gz", scenario));
        Network network = NetworkUtils.readNetwork(networkPath.toString());
        Map<Id<Link>, AreaKind> areaKindByLink = getAreaKindByLink(network);

        Path eventsPath = outputPath.resolve(String.format("%s.output_events.xml.gz", scenario));
        EventHandler eventHandler = new EventHandler(areaKindByLink);
        EventsManager eventsManager = EventsUtils.createEventsManager();
        eventsManager.addHandler(eventHandler);

        EventsUtils.readEvents(eventsManager, eventsPath.toString());
        Path analysisPath = scenarioPath.resolve("analysis");
        boolean analysisDirectoryCreated = analysisPath.toFile().mkdirs();
        if (analysisDirectoryCreated) {
            log.info(String.format("Directory %s created", scenarioPath));
        }
        Path csvPath = analysisPath.resolve(String.format("%s.trips_berlinwise.csv", scenario));
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
                System.out.printf("Running on %s%n", scenario.getScenarioName());
                runAnalysis(Path.of(args[0]), scenario.getScenarioName());
            }
        } else {
            runAnalysis(Path.of(args[0]), args[1]);
        }
    }

}
