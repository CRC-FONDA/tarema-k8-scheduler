package fonda.scheduler.distributedscheduler;

import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.PodListWithIndex;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeList;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class SJFNScheduler {

    public static PodList unscheduledPods = new PodList();

    public static NodeList nodeList;

    public static PodListWithIndex podList;

    private static final Logger logger = LoggerFactory.getLogger(SJFNScheduler.class);

    public static synchronized Optional<Pair<Pod, Node>> scheduleSJFN(Pod podToSchedule) {
        if (podToSchedule != null) {
            // logger.info(podToSchedule.getSpec().getContainers().get(0).getName() + " - " +"Schedule pod " + unscheduledPods.getItems().size() + " elements.");
            return scheduleSJFN(podList, nodeList, podToSchedule);
        } else {
            logger.info("Schedule Queue with " + unscheduledPods.getItems().size() + " elements.");
            scheduleQueue(podList, nodeList);
            return Optional.empty();
        }
    }

    static Optional<Pair<Pod, Node>> scheduleSJFN(PodList existingPods, NodeList existingNodes, Pod podToSchedule) {

        List<NodeWithAlloc> nodesWithAllocList = K8Helper.estimateFreeNodeCapabilities(existingPods, existingNodes);

        TreeMap<NodeWithAlloc, Double> nodeScore = K8Helper.calculateNodeScore(nodesWithAllocList, podToSchedule);

        NodeWithAlloc mostPowerfulNode = K8Helper.getMostPowerfulNode(new ArrayList<>(nodeScore.keySet()));

        if (nodeScore.isEmpty()) {
            logger.info(podToSchedule.getSpec().getContainers().get(0).getName() + " - " + "Pod " + K8Helper.getFullPodName(podToSchedule) + " was unable to get scheduled due to insufficient resources. Added to unscheduled queue.");
            unscheduledPods.getItems().add(podToSchedule);
            return Optional.empty();
        } else {
            //with fair scheduling -> default for Tarema and the fair scheduler

            // NodeWithAlloc maxNode = pair.getValue0();

            // without fair scheduling -> fill nodes approach
            //NodeWithAlloc maxNode =  Collections.min(nodeScore.entrySet(), Comparator.comparingInt(Map.Entry::getValue)).getKey();

            return Optional.of(K8Helper.bindPodToNode(podToSchedule, mostPowerfulNode.getNode(), -1.0));

        }

    }

    static void scheduleQueue(PodList existingPods, NodeList existingNodes) {
        if (unscheduledPods.getItems().isEmpty()) {
            return;
        }

        List<NodeWithAlloc> nodesWithAllocList = K8Helper.estimateFreeNodeCapabilities(existingPods, existingNodes);

        AtomicBoolean ongoing = new AtomicBoolean(true);
        unscheduledPods.getItems().stream().sorted((o1, o2) -> {
            if (Double.valueOf(o1.getSpec().getAffinity().getNodeAffinity().getPreferredDuringSchedulingIgnoredDuringExecution().get(2).getPreference().getMatchExpressions().get(0).getValues().get(0)) >
                    Double.valueOf(o2.getSpec().getAffinity().getNodeAffinity().getPreferredDuringSchedulingIgnoredDuringExecution().get(2).getPreference().getMatchExpressions().get(0).getValues().get(0))) {
                return 1;
            } else {
                return -1;
            }
        }).takeWhile(t -> ongoing.get() ==true).forEach(pod -> {
            TreeMap<NodeWithAlloc, Double> singleNodePodScore = K8Helper.calculateNodeScore(nodesWithAllocList, pod);

            if (!singleNodePodScore.isEmpty()) {
                NodeWithAlloc mostPowerfulNode = K8Helper.getMostPowerfulNode(new ArrayList<>(singleNodePodScore.keySet()));
                logger.info("[" + pod.getSpec().getContainers().get(0).getName() + "] - " + "Schedule pod " + K8Helper.getFullPodName(pod) + " to " + pod.getMetadata().getName() + " from queue.");
                Pair<Pod, Node> pair = K8Helper.bindPodToNode(pod, mostPowerfulNode.getNode(), -1.0);
                unscheduledPods.getItems().remove(pod);
                Pod podToAdd = pair.getValue0();
                podToAdd.getSpec().setNodeName(pair.getValue1().getMetadata().getName());
                SJFNScheduler.podList.addPodToList(podToAdd);
                ongoing.set(false);
            }
        });

    }

}
