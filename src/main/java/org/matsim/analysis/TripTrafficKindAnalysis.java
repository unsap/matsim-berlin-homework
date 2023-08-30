package org.matsim.analysis;

import com.google.common.collect.Iterables;
import org.apache.log4j.Logger;
import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.events.*;
import org.matsim.api.core.v01.events.handler.*;
import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.network.Network;
import org.matsim.api.core.v01.population.Person;
import org.matsim.core.api.experimental.events.EventsManager;
import org.matsim.core.events.EventsUtils;
import org.matsim.core.network.NetworkUtils;
import org.matsim.core.router.TripStructureUtils;
import org.matsim.prepare.ScenarioCreator.AreaKind;
import org.matsim.vehicles.Vehicle;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public class TripTrafficKindAnalysis {

    private final static Logger log = Logger.getLogger(TripTrafficKindAnalysis.class);

    private enum TrafficKind {

        BERLIN_ORIGIN,
        BERLIN_DESTINATION,
        BERLIN_INTERNAL,
        BERLIN_THROUGH,
        NON_BERLIN;

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
                    return BERLIN_THROUGH;
                } else {
                    return NON_BERLIN;
                }
            }
        }

    }

    private static class TripData {

        private final Id<Person> personId;
        private final int tripNumber;

        private Id<Link> startLinkId = null;
        private Boolean startLinkInBerlin = null;
        private boolean atLeastOneLinkInBerlin = false;
        private Id<Link> lastLinkId = null;
        private Boolean lastLinkInBerlin = null;
        /**
         * A trip is completed after an activity has started which does not belong to the interaction activities
         */
        private boolean completed = false;

        private TripData(Id<Person> personId, int tripNumber) {
            this.personId = personId;
            this.tripNumber = tripNumber;
        }

        public String getCsvRow() {
            TrafficKind trafficKind = TrafficKind.fromValues(startLinkInBerlin, atLeastOneLinkInBerlin, lastLinkInBerlin);
            String trafficKindValue;
            if (trafficKind == null) {
                trafficKindValue = null;
            } else {
                trafficKindValue = trafficKind.toString().toLowerCase();
            }
            return String.format("%s;%s;%s_%s;%s;%s;%s", personId, tripNumber, personId, tripNumber,
                    startLinkId, lastLinkId, trafficKindValue);
        }

    }

    private static class EventHandler implements
            PersonEntersVehicleEventHandler, LinkLeaveEventHandler, LinkEnterEventHandler, PersonLeavesVehicleEventHandler, ActivityStartEventHandler {

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
        public void handleEvent(PersonEntersVehicleEvent event) {
            Id<Person> personId = event.getPersonId();
            List<TripData> tripsOfPerson = tripsByPerson.computeIfAbsent(personId, p -> new ArrayList<>());
            TripData trip = getIncompleteTrip(tripsOfPerson)
                    .orElseGet(() -> {
                        int tripNumber = tripsOfPerson.size() + 1;
                        TripData newTrip = new TripData(personId, tripNumber);
                        tripsOfPerson.add(newTrip);
                        return newTrip;
                    });

            Id<Vehicle> vehicleId = event.getVehicleId();
            tripsByVehiclesInTraffic.computeIfAbsent(vehicleId, v -> new HashMap<>()).put(personId, trip);
        }

        @Override
        public void handleEvent(LinkLeaveEvent event) {
            Id<Link> linkId = event.getLinkId();
            AreaKind areaKind = areaKindByLink.get(linkId);
            for (TripData tripData : tripsByVehiclesInTraffic.get(event.getVehicleId()).values()) {
                if (tripData.startLinkId == null) {
                    tripData.startLinkId = linkId;
                    tripData.startLinkInBerlin = areaKind.isInBerlin();

                }
                tripData.atLeastOneLinkInBerlin |= areaKind.isInBerlin();
                tripData.lastLinkId = linkId;
                tripData.lastLinkInBerlin = areaKind.isInBerlin();
            }
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
                    TripData lastTripOfPerson = Iterables.getLast(tripsOfPerson);
                    lastTripOfPerson.completed = true;
                }
            }
        }

        public void writeCsv(Writer writer) throws IOException {
            writer.write("person;trip_number;trip_id;start_link;end_link;traffic_kind" + System.lineSeparator());
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
        Path csvPath = scenarioPath.resolve("analysis").resolve(String.format("%s.trips_traffic_kind.csv", scenario));
        boolean analysisDirectoryCreated = csvPath.toFile().mkdirs();
        if (analysisDirectoryCreated) {
            log.info(String.format("Directory %s created", scenarioPath));
        }
        try (Writer writer = new FileWriter(csvPath.toFile())) {
            eventHandler.writeCsv(writer);
        }
    }

    /**
     * Run this with:
     * 1. The path to the scenarios directory
     * 2. The name of the scenario you want to check
     */
    public static void main(String[] args) throws IOException {
        runAnalysis(Path.of(args[0]), args[1]);
    }

}
