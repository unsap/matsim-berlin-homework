package org.matsim.analysis;

import org.apache.log4j.Logger;
import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.events.PersonEntersVehicleEvent;
import org.matsim.api.core.v01.events.PersonLeavesVehicleEvent;
import org.matsim.api.core.v01.events.handler.PersonEntersVehicleEventHandler;
import org.matsim.api.core.v01.events.handler.PersonLeavesVehicleEventHandler;
import org.matsim.core.api.experimental.events.EventsManager;
import org.matsim.core.events.EventsUtils;
import org.matsim.vehicles.MatsimVehicleReader;
import org.matsim.vehicles.Vehicle;
import org.matsim.vehicles.VehicleUtils;
import org.matsim.vehicles.Vehicles;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Checks the occupancy degree of cars.
 */
public class CarOccupancyCheck {

    private final static Logger log = Logger.getLogger(CarOccupancyCheck.class);

    private static class CarOccupancyCheckEventHandler implements PersonEntersVehicleEventHandler, PersonLeavesVehicleEventHandler {

        private final Vehicles vehicles;
        private final Map<Id<Vehicle>, Integer> occupancyDegreeByVehicle = new HashMap<>();

        private CarOccupancyCheckEventHandler(Vehicles vehicles) {
            this.vehicles = vehicles;
        }

        private boolean isNotCar(Id<Vehicle> vehicleId) {
            Vehicle vehicle = vehicles.getVehicles().get(vehicleId);
            if (vehicle == null) {
                // We only read the vehicles file, not the transit vehicles file.
                // So when something does not show up in the vehicles file, it is not a car.
                return true;
            }
            return !vehicle.getType().getId().toString().equals("car");
        }

        @Override
        public void handleEvent(PersonEntersVehicleEvent event) {
            Id<Vehicle> vehicleId = event.getVehicleId();
            if (isNotCar(vehicleId)) {
                return;
            }
            if (!occupancyDegreeByVehicle.containsKey(vehicleId)) {
                occupancyDegreeByVehicle.put(vehicleId, 1);
            } else {
                Integer occupancy = occupancyDegreeByVehicle.get(vehicleId) + 1;
                occupancyDegreeByVehicle.put(vehicleId, occupancy);
                if (occupancy > 1) {
                    log.warn(String.format("Vehicle %s has occupancy of %d", vehicleId, occupancy));
                }
            }
        }

        @Override
        public void handleEvent(PersonLeavesVehicleEvent event) {
            Id<Vehicle> vehicleId = event.getVehicleId();
            if (isNotCar(vehicleId)) {
                return;
            }
            Integer occupancy = occupancyDegreeByVehicle.get(vehicleId) - 1;
            occupancyDegreeByVehicle.put(vehicleId, occupancy);
        }

    }

    public static void runCheck(Path scenariosPath, String abbreviation) {
        Path outputPath = scenariosPath.resolve(String.format("berlin-v5.5-%s", abbreviation)).resolve("output");

        Path vehiclesPath = outputPath.resolve(String.format("berlin-v5.5-%s.output_vehicles.xml.gz", abbreviation));
        Vehicles vehicles = VehicleUtils.createVehiclesContainer();
        MatsimVehicleReader vehicleReader = new MatsimVehicleReader(vehicles);
        vehicleReader.readFile(vehiclesPath.toString());

        Path eventsPath = outputPath.resolve(String.format("berlin-v5.5-%s.output_events.xml.gz", abbreviation));
        CarOccupancyCheckEventHandler eventHandler = new CarOccupancyCheckEventHandler(vehicles);
        EventsManager eventsManager = EventsUtils.createEventsManager();
        eventsManager.addHandler(eventHandler);

        EventsUtils.readEvents(eventsManager, eventsPath.toString());
    }

    /**
     * Run this with:
     * 1. The path to the scenarios directory
     * 2. The abbreviation of the scenario you want to check
     */
    public static void main(String[] args) {
        runCheck(Path.of(args[0]), args[1]);
    }

}
