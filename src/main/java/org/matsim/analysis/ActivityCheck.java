package org.matsim.analysis;

import org.apache.log4j.Logger;
import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.events.ActivityStartEvent;
import org.matsim.api.core.v01.events.PersonDepartureEvent;
import org.matsim.api.core.v01.events.handler.ActivityStartEventHandler;
import org.matsim.api.core.v01.events.handler.PersonDepartureEventHandler;
import org.matsim.api.core.v01.population.Activity;
import org.matsim.api.core.v01.population.Leg;
import org.matsim.api.core.v01.population.Person;
import org.matsim.api.core.v01.population.PlanElement;
import org.matsim.core.api.experimental.events.EventsManager;
import org.matsim.core.config.Config;
import org.matsim.core.config.ConfigUtils;
import org.matsim.core.events.EventsUtils;
import org.matsim.core.population.algorithms.PersonAlgorithm;
import org.matsim.core.population.io.StreamingPopulationReader;
import org.matsim.core.scenario.ScenarioUtils;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Checks whether activity got dropped, but not their trips
 */
public class ActivityCheck {

    private final static Logger log = Logger.getLogger(ActivityCheck.class);

    /**
     * Stores only the plan elements of the selected plan to hopefully save some memory
     */
    private static class StoreSelectedPlanPersonAlgorithm
            implements PersonAlgorithm {

        private final Map<Id<Person>, List<PlanElement>> selectedPlansByPerson = new HashMap<>();

        @Override
        public void run(Person person) {
            selectedPlansByPerson.put(person.getId(), person.getSelectedPlan().getPlanElements());
        }

    }

    private static class ActivityCheckEventHandler
            implements PersonDepartureEventHandler, ActivityStartEventHandler {

        private final Map<Id<Person>, List<PlanElement>> selectedPlanByPerson;
        private final Map<Id<Person>, Integer> planProgressByPerson;

        private ActivityCheckEventHandler(Map<Id<Person>, List<PlanElement>> selectedPlanByPerson) {
            this.selectedPlanByPerson = selectedPlanByPerson;
            this.planProgressByPerson = selectedPlanByPerson.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> 1));
        }

        @Override
        public void handleEvent(ActivityStartEvent event) {
            Id<Person> personId = event.getPersonId();
            List<PlanElement> plan = selectedPlanByPerson.get(personId);
            int planProgress = planProgressByPerson.get(personId);
            Activity plannedActivity = (Activity) plan.get(planProgress);
            if (!event.getActType().equals(plannedActivity.getType()) || !event.getLinkId().equals(plannedActivity.getLinkId())) {
                log.error(String.format("Activity does not match: %s %s", plannedActivity, event));
            }
            planProgressByPerson.put(personId, planProgress + 1);
        }

        @Override
        public void handleEvent(PersonDepartureEvent event) {
            Id<Person> personId = event.getPersonId();
            List<PlanElement> plan = selectedPlanByPerson.get(personId);
            int planProgress = planProgressByPerson.get(personId);
            Leg plannedLeg = (Leg) plan.get(planProgress);
            if (!event.getLegMode().equals(plannedLeg.getMode()) || !event.getLinkId().equals(plannedLeg.getRoute().getStartLinkId())) {
                log.error(String.format("Leg does not match: %s %s", plannedLeg, event));
            }
            planProgressByPerson.put(personId, planProgress + 1);
        }

        public void checkPlanCompletion() {
            for (Id<Person> personId : selectedPlanByPerson.keySet()) {
                List<PlanElement> plan = selectedPlanByPerson.get(personId);
                int planProgress = planProgressByPerson.get(personId);
                if (planProgress != plan.size()) {
                    log.error(String.format("Person %s has only completed %d / %d plan elements%n%s", personId, planProgress, plan.size(), plan.stream()
                            .map(Object::toString).collect(Collectors.joining(System.lineSeparator()))));
                }
            }
        }

    }

    public static void runCheck(Path scenariosPath, String scenario) {
        Path outputPath = scenariosPath.resolve(scenario).resolve("output");

        Path plansPath = outputPath.resolve(String.format("%s.output_plans.xml.gz", scenario));
        Config config = ConfigUtils.createConfig();
        StreamingPopulationReader streamingPopulationReader = new StreamingPopulationReader(ScenarioUtils.createScenario(config));
        StoreSelectedPlanPersonAlgorithm storeSelectedPlanPersonAlgorithm = new StoreSelectedPlanPersonAlgorithm();
        streamingPopulationReader.addAlgorithm(storeSelectedPlanPersonAlgorithm);
        streamingPopulationReader.readFile(plansPath.toString());
        Map<Id<Person>, List<PlanElement>> selectedPlanByPerson = storeSelectedPlanPersonAlgorithm.selectedPlansByPerson;

        Path eventsPath = outputPath.resolve(String.format("%s.output_events.xml.gz", scenario));
        ActivityCheckEventHandler eventHandler = new ActivityCheckEventHandler(selectedPlanByPerson);
        EventsManager eventsManager = EventsUtils.createEventsManager();
        eventsManager.addHandler(eventHandler);

        EventsUtils.readEvents(eventsManager, eventsPath.toString());
        eventHandler.checkPlanCompletion();
    }

    /**
     * Run this with:
     * 1. The path to the scenarios directory
     * 2. The name of the scenario you want to check
     */
    public static void main(String[] args) {
        runCheck(Path.of(args[0]), args[1]);
    }

}
