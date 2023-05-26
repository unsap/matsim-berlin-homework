package org.matsim.analysis;

import com.google.common.base.Functions;
import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.events.LinkEnterEvent;
import org.matsim.api.core.v01.events.VehicleEntersTrafficEvent;
import org.matsim.api.core.v01.events.handler.LinkEnterEventHandler;
import org.matsim.api.core.v01.events.handler.VehicleEntersTrafficEventHandler;
import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.network.Network;
import org.matsim.core.events.EventsUtils;
import org.matsim.core.network.NetworkUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
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
        private int vehicleCount = 0;

        private LinkData(String suffix) {
            this.suffix = suffix;
        }

        public static LinkData createDifference(LinkData base, LinkData policy) {
            var difference = new LinkData("Difference");
            difference.vehicleCount = policy.vehicleCount - base.vehicleCount;
            return difference;
        }

        public void modifyLink(Link link) {
            link.getAttributes().putAttribute(String.format("vehicleCount%s", suffix), vehicleCount);
        }

    }

    private static class LinkAnalysisEventHandler implements VehicleEntersTrafficEventHandler, LinkEnterEventHandler {

        private final Map<Id<Link>, LinkData> linkData;

        public LinkAnalysisEventHandler(Network network, String suffix) {
            this.linkData = network.getLinks().entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> new LinkData(suffix)));
        }

        public static Map<Id<Link>, LinkData> analyzeLinkData(Network network, Path events_path, String suffix) {
            var event_handler = new LinkAnalysisEventHandler(network, suffix);
            var event_manager = EventsUtils.createEventsManager();
            event_manager.addHandler(event_handler);

            EventsUtils.readEvents(event_manager, events_path.toString());
            return event_handler.linkData;
        }


        @Override
        public void handleEvent(LinkEnterEvent event) {
            linkData.get(event.getLinkId()).vehicleCount += 1;
        }

        @Override
        public void handleEvent(VehicleEntersTrafficEvent event) {
            linkData.get(event.getLinkId()).vehicleCount += 1;
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
