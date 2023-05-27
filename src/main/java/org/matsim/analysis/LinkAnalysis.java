package org.matsim.analysis;

import com.google.common.base.Functions;
import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.events.LinkEnterEvent;
import org.matsim.api.core.v01.events.LinkLeaveEvent;
import org.matsim.api.core.v01.events.VehicleEntersTrafficEvent;
import org.matsim.api.core.v01.events.VehicleLeavesTrafficEvent;
import org.matsim.api.core.v01.events.handler.LinkEnterEventHandler;
import org.matsim.api.core.v01.events.handler.LinkLeaveEventHandler;
import org.matsim.api.core.v01.events.handler.VehicleEntersTrafficEventHandler;
import org.matsim.api.core.v01.events.handler.VehicleLeavesTrafficEventHandler;
import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.network.Network;
import org.matsim.core.events.EventsUtils;
import org.matsim.core.network.NetworkUtils;
import org.matsim.utils.objectattributes.attributable.Attributes;
import org.matsim.vehicles.Vehicle;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LinkAnalysis {

    private final Network network;
    private final Map<Id<Link>, LinkData> base;
    private final Map<Id<Link>, LinkData> policy;
    private final Map<Id<Link>, LinkData> difference;

    private static class LinkData {

        private final String suffix;
        /**
         * Number of vehicles which used this link during the whole scenario.
         */
        private final int vehicleCount;
        /**
         * Average time vehicles needed to traverse this link.
         * Excludes vehicles which entered / left the link between its boundary nodes.
         * Is {@code null} when no vehicles traversed this link.
         */
        private final @Nullable Double averageTravelTime;
        /**
         * Longest time a vehicle needed to traverse this link.
         * Excludes vehicles which entered / left the link between its boundary nodes.
         * Is {@code null} when no vehicles traversed this link.
         */
        private final @Nullable Double maxTravelTime;

        private LinkData(String suffix, int vehicleCount, @Nullable Double averageTravelTime, @Nullable Double maxTravelTime) {
            this.suffix = suffix;
            this.vehicleCount = vehicleCount;
            this.averageTravelTime = averageTravelTime;
            this.maxTravelTime = maxTravelTime;
        }

        public static LinkData createDifference(LinkData base, LinkData policy) {
            int vehicleCount = policy.vehicleCount - base.vehicleCount;
            Double averageTravelTime = subtract(policy.averageTravelTime, base.averageTravelTime);
            Double maxTravelTime = subtract(policy.maxTravelTime, base.maxTravelTime);
            return new LinkData("Difference", vehicleCount, averageTravelTime, maxTravelTime);
        }

        private static @Nullable Double subtract(@Nullable Double minuend, @Nullable Double subtrahend) {
            if (minuend == null || subtrahend == null) {
                return null;
            } else {
                return minuend - subtrahend;
            }
        }

        public void modifyLink(Link link) {
            Attributes attributes = link.getAttributes();
            attributes.putAttribute(String.format("vehicleCount%s", suffix), vehicleCount);
            if (averageTravelTime != null) {
                attributes.putAttribute(String.format("averageTravelTime%s", suffix), averageTravelTime);
            }
            if (maxTravelTime != null) {
                attributes.putAttribute(String.format("maxTravelTime%s", suffix), maxTravelTime);
            }
        }

    }

    private static class LinkDataBuilder {

        private int vehicleCount = 0;
        private final Map<Id<Vehicle>, Double> vehicleEnterTimes = new HashMap<>();
        private final List<Double> travelTimes = new ArrayList<>();

        public LinkData build(String suffix) {
            Double averageTravelTime;
            if (vehicleCount == 0) {
                averageTravelTime = null;
            } else {
                averageTravelTime = travelTimes.stream().reduce(0.0, Double::sum) / vehicleCount;
            }
            Double maxTravelTime = travelTimes.stream()
                    .max(Double::compareTo).orElse(null);
            return new LinkData(suffix, vehicleCount, averageTravelTime, maxTravelTime);
        }

    }

    private static class LinkAnalysisEventHandler implements VehicleEntersTrafficEventHandler, LinkEnterEventHandler, LinkLeaveEventHandler, VehicleLeavesTrafficEventHandler {

        private final Map<Id<Link>, LinkDataBuilder> linkDataBuilders;

        public LinkAnalysisEventHandler(Network network) {
            this.linkDataBuilders = network.getLinks().entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> new LinkDataBuilder()));
        }

        public static Map<Id<Link>, LinkData> analyzeLinkData(Network network, Path events_path, String suffix) {
            var event_handler = new LinkAnalysisEventHandler(network);
            var event_manager = EventsUtils.createEventsManager();
            event_manager.addHandler(event_handler);

            EventsUtils.readEvents(event_manager, events_path.toString());
            return event_handler.linkDataBuilders.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().build(suffix)));
        }

        @Override
        public void handleEvent(VehicleEntersTrafficEvent event) {
            linkDataBuilders.get(event.getLinkId()).vehicleCount += 1;
        }

        @Override
        public void handleEvent(LinkEnterEvent event) {
            LinkDataBuilder linkDataBuilder = linkDataBuilders.get(event.getLinkId());
            linkDataBuilder.vehicleEnterTimes.put(event.getVehicleId(), event.getTime());
            linkDataBuilder.vehicleCount += 1;
        }

        @Override
        public void handleEvent(LinkLeaveEvent event) {
            LinkDataBuilder linkDataBuilder = linkDataBuilders.get(event.getLinkId());
            Double enterTime = linkDataBuilder.vehicleEnterTimes.remove(event.getVehicleId());
            if (enterTime != null) {
                linkDataBuilder.travelTimes.add(event.getTime() - enterTime);
            }
        }

        @Override
        public void handleEvent(VehicleLeavesTrafficEvent event) {
            LinkDataBuilder linkDataBuilder = linkDataBuilders.get(event.getLinkId());
            linkDataBuilder.vehicleEnterTimes.remove(event.getVehicleId());
        }

    }

    public LinkAnalysis(Path network_path, Path events_path_base, Path events_path_policy) {
        network = NetworkUtils.readNetwork(network_path.toString());
        base = LinkAnalysisEventHandler.analyzeLinkData(network, events_path_base, "Base");
        policy = LinkAnalysisEventHandler.analyzeLinkData(network, events_path_policy, "Policy");
        difference = base.keySet().stream()
                .collect(Collectors.toMap(Functions.identity(), link -> LinkData.createDifference(base.get(link), policy.get(link))));
    }

    public void writeModifiedNetwork(Path network_analyzed_path) {
        for (var linkData : List.of(base, policy, difference)) {
            for (var entry : linkData.entrySet()) {
                var link = network.getLinks().get(entry.getKey());
                entry.getValue().modifyLink(link);
            }
        }
        NetworkUtils.writeNetwork(network, network_analyzed_path.toString());
    }

    public static void main(String[] args) throws IOException {
        var network_path = Paths.get("scenarios", "berlin-annotated-10pct-100i", "berlin-v5.5-network-annotated.xml.gz");
        var events_path_base = Paths.get("scenarios", "berlin-base-10pct-100i", "output", "berlin-v5.5-10pct.output_events.xml.gz");
        var events_path_policy = Paths.get("scenarios", "berlin-annotated-10pct-100i", "output", "berlin-v5.5-10pct.output_events.xml.gz");
        var analysis = new LinkAnalysis(network_path, events_path_base, events_path_policy);
        analysis.writeModifiedNetwork(network_path.getParent().resolve("berlin-v5.5-network-analyzed.xml.gz"));
    }

}
