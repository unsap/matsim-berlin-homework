package org.matsim.analysis;

import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.events.HasLinkId;
import org.matsim.api.core.v01.events.LinkEnterEvent;
import org.matsim.api.core.v01.events.VehicleEntersTrafficEvent;
import org.matsim.api.core.v01.events.VehicleLeavesTrafficEvent;
import org.matsim.api.core.v01.events.handler.LinkEnterEventHandler;
import org.matsim.api.core.v01.events.handler.VehicleEntersTrafficEventHandler;
import org.matsim.api.core.v01.events.handler.VehicleLeavesTrafficEventHandler;
import org.matsim.api.core.v01.network.Network;
import org.matsim.api.core.v01.population.Person;
import org.matsim.core.events.EventsUtils;
import org.matsim.core.network.NetworkUtils;
import org.matsim.core.population.PopulationUtils;
import org.matsim.vehicles.Vehicle;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Paths;
import java.util.*;

public class AffectedAgentsAnalysis {

    private static class PersonData {

        private final Person person;
        private boolean hasUsedModifiedLink = false;

        private PersonData(Person person) {
            this.person = person;
        }

        public void setHasUsedModifiedLink() {
            this.hasUsedModifiedLink = true;
        }

        private static final String CSV_HEADER = "person_id;has_used_modified_link";

        private String buildCsvRow() {
            return String.format("%s;%d", person.getId(), hasUsedModifiedLink ? 1 : 0);
        }

        public static void writeCsv(Collection<PersonData> persons, Writer writer) throws IOException {
            writer.write(CSV_HEADER);
            writer.write("\n");
            for (var person : persons) {
                writer.write(person.buildCsvRow());
                writer.write("\n");
            }
        }

    }

    private static class AffectedAgentsEventHandler implements VehicleEntersTrafficEventHandler, LinkEnterEventHandler, VehicleLeavesTrafficEventHandler {

        private final Map<Id<Person>, PersonData> population;
        private final Map<Id<Vehicle>, Set<Id<Person>>> vehiclePassengers = new HashMap<>();
        private final Network network;

        private AffectedAgentsEventHandler(Map<Id<Person>, PersonData> population, Network network) {
            this.population = population;
            this.network = network;
        }

        private boolean usesModifiedLink(HasLinkId event) {
            return (boolean) network.getLinks().get(event.getLinkId()).getAttributes().getAttribute("isModified");
        }

        @Override
        public void handleEvent(LinkEnterEvent event) {
            if (usesModifiedLink(event)) {
                var passengers = vehiclePassengers.get(event.getVehicleId());
                for (var person : passengers) {
                    population.get(person).setHasUsedModifiedLink();
                }
            }
        }

        @Override
        public void handleEvent(VehicleEntersTrafficEvent event) {
            if (usesModifiedLink(event)) {
                population.get(event.getPersonId()).setHasUsedModifiedLink();
            }
            vehiclePassengers.computeIfAbsent(event.getVehicleId(), vehicleId -> new HashSet<>()).add(event.getPersonId());
        }

        @Override
        public void handleEvent(VehicleLeavesTrafficEvent event) {
            vehiclePassengers.get(event.getVehicleId()).remove(event.getPersonId());
        }

    }

    public static void main(String[] args) throws IOException {
        var population_path = Paths.get("scenarios", "berlin-base-10pct-100i", "output", "berlin-v5.5-10pct.output_plans.xml.gz");
        Map<Id<Person>, PersonData> population = new HashMap<>();
        for (var person : PopulationUtils.readPopulation(population_path.toString()).getPersons().values()) {
            population.put(person.getId(), new PersonData(person));
        }

        var network_path = Paths.get("scenarios", "berlin-annotated-10pct-100i", "berlin-v5.5-network-annotated.xml.gz");
        var network = NetworkUtils.readNetwork(network_path.toString());

        var event_handler = new AffectedAgentsEventHandler(population, network);
        var event_manager = EventsUtils.createEventsManager();
        event_manager.addHandler(event_handler);
        var events_path = Paths.get("scenarios", "berlin-base-10pct-100i", "output", "berlin-v5.5-10pct.output_events.xml.gz");
        EventsUtils.readEvents(event_manager, events_path.toString());

        try (FileWriter fileWriter = new FileWriter("persons.csv")) {
            PersonData.writeCsv(event_handler.population.values(), fileWriter);
        }
    }


}
