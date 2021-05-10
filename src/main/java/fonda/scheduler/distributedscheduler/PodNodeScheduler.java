package fonda.scheduler.distributedscheduler;

import fonda.scheduler.controller.KubernetesClientSingleton;
import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.PodListWithIndex;
import io.fabric8.kubernetes.api.model.*;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.*;
import java.util.concurrent.atomic.AtomicReference;


public class PodNodeScheduler {

    public static PodList unscheduledPods = new PodList();

    public static NodeList nodeList;

    public static PodListWithIndex podList;

    private static int nodeIndex = 0;

    private static final Logger logger = LoggerFactory.getLogger(PodNodeScheduler.class);

    private PodNodeScheduler() {

    }

    public static synchronized Optional<Pair<Pod, Node>> schedule(Pod podToSchedule) {
        if (podToSchedule != null) {
            return schedule(podList, nodeList, podToSchedule);
        } else {
            logger.info("Schedule Queue with " + unscheduledPods.getItems().size() + " elements.");
            scheduleQueue(podList, nodeList);
            return Optional.empty();
        }
    }

    public static synchronized Optional<Pair<Pod, Node>> scheduleRR(Pod podToSchedule, String str) {
        if (podToSchedule != null) {
            // logger.info(podToSchedule.getSpec().getContainers().get(0).getName() + " - " +"Schedule pod " + unscheduledPods.getItems().size() + " elements.");
            return scheduleRR(podList, nodeList, podToSchedule, 0);
        } else {
            logger.info("Schedule Queue with " + unscheduledPods.getItems().size() + " elements.");
            scheduleQueue(podList, nodeList);
            return Optional.empty();
        }
    }

    public static synchronized Optional<Pair<Pod, Node>> scheduleSJFN(Pod podToSchedule, String str) {
        if (podToSchedule != null) {
            // logger.info(podToSchedule.getSpec().getContainers().get(0).getName() + " - " +"Schedule pod " + unscheduledPods.getItems().size() + " elements.");
            return scheduleSJFN(podList, nodeList, podToSchedule);
        } else {
            logger.info("Schedule Queue with " + unscheduledPods.getItems().size() + " elements.");
            scheduleQueue(podList, nodeList);
            return Optional.empty();
        }
    }


    static Optional<Pair<Pod, Node>> schedule(PodList existingPods, NodeList existingNodes, Pod podToSchedule) {

        List<NodeWithAlloc> nodesWithAllocList = K8Helper.estimateFreeNodeCapabilities(existingPods, existingNodes);

        TreeMap<NodeWithAlloc, Double> nodeScore = K8Helper.calculateNodeScore(nodesWithAllocList, podToSchedule);

        if (nodeScore.isEmpty()) {
            logger.info(podToSchedule.getSpec().getContainers().get(0).getName() + " - " + "Pod " + K8Helper.getFullPodName(podToSchedule) + " was unable to get scheduled due to insufficient resources. Added to unscheduled queue.");
            unscheduledPods.getItems().add(podToSchedule);
            return Optional.empty();
        } else {
            //with fair scheduling -> default for Tarema and the fair scheduler
            var pair = K8Helper.findOptimalNodeWithAlloc(nodeScore);

            NodeWithAlloc maxNode = pair.getValue0();

            // without fair scheduling -> fill nodes approach
            //NodeWithAlloc maxNode =  Collections.min(nodeScore.entrySet(), Comparator.comparingInt(Map.Entry::getValue)).getKey();

            return Optional.of(K8Helper.bindPodToNode(podToSchedule, maxNode.getNode(), pair.getValue1()));

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
            var pair = K8Helper.findOptimalNodeWithAlloc(nodeScore);

           // NodeWithAlloc maxNode = pair.getValue0();

            // without fair scheduling -> fill nodes approach
            //NodeWithAlloc maxNode =  Collections.min(nodeScore.entrySet(), Comparator.comparingInt(Map.Entry::getValue)).getKey();

            return Optional.of(K8Helper.bindPodToNode(podToSchedule, mostPowerfulNode.getNode(), -1.0));

        }

    }



    static Optional<Pair<Pod, Node>> scheduleRR(PodList existingPods, NodeList existingNodes, Pod podToSchedule, int counter) {

        if (counter == existingNodes.getItems().size()) {
            unscheduledPods.getItems().add(podToSchedule);
            return Optional.empty();
        }

        if (nodeIndex >= existingNodes.getItems().size()) {
            nodeIndex = 0;
        }

        List<NodeWithAlloc> nodesWithAllocList = K8Helper.estimateFreeNodeCapabilities(existingPods, existingNodes.getItems().get(nodeIndex));

        TreeMap<NodeWithAlloc, Double> nodeScore = K8Helper.calculateNodeScore(nodesWithAllocList, podToSchedule);

        if (nodeScore.isEmpty()) {
            nodeIndex++;
            return scheduleRR(existingPods, existingNodes, podToSchedule, counter + 1);
        } else {
            nodeIndex++;
            return Optional.of(K8Helper.bindPodToNode(podToSchedule, nodesWithAllocList.get(0).getNode(), -1.0));
        }

    }


    static void scheduleQueue(PodList existingPods, NodeList existingNodes) {

        if (unscheduledPods.getItems().isEmpty()) {
            return;
        }

        List<NodeWithAlloc> nodesWithAllocList = K8Helper.estimateFreeNodeCapabilities(existingPods, existingNodes);

        AtomicReference<Triplet<Pod, Node, Double>> nodePodWithHighestScore = new AtomicReference<>(new Triplet<>(null, null, 10.0));

        unscheduledPods.getItems().forEach(pod -> {

            TreeMap<NodeWithAlloc, Double> singleNodePodScore = K8Helper.calculateNodeScore(nodesWithAllocList, pod);
            if (!singleNodePodScore.isEmpty()) {
                var singleNodePodMaxScore = Collections.min(singleNodePodScore.entrySet(), Comparator.comparingDouble(Map.Entry::getValue));

                if (singleNodePodMaxScore.getValue() < nodePodWithHighestScore.get().getValue2()) {
                    nodePodWithHighestScore.set(new Triplet<>(pod, singleNodePodMaxScore.getKey().getNode(), singleNodePodMaxScore.getValue()));


                }
            }
        });

        if (nodePodWithHighestScore.get().getValue2() < 100.0) {
            logger.info("[" + nodePodWithHighestScore.get().getValue0().getSpec().getContainers().get(0).getName() + "] - " + "Schedule pod " + K8Helper.getFullPodName(nodePodWithHighestScore.get().getValue0()) + " to " + nodePodWithHighestScore.get().getValue1().getMetadata().getName() + " from queue.");
            Pair<Pod, Node> pair = K8Helper.bindPodToNode(nodePodWithHighestScore.get().getValue0(), nodePodWithHighestScore.get().getValue1(), nodePodWithHighestScore.get().getValue2());
            unscheduledPods.getItems().remove(nodePodWithHighestScore.get().getValue0());
            Pod podToAdd = pair.getValue0();
            podToAdd.getSpec().setNodeName(pair.getValue1().getMetadata().getName());
            PodNodeScheduler.podList.addPodToList(podToAdd);
        }

    }

    static void scheduleQueueFIFO(PodList existingPods, NodeList existingNodes) {

        if (unscheduledPods.getItems().isEmpty()) {
            return;
        }

        List<NodeWithAlloc> nodesWithAllocList = K8Helper.estimateFreeNodeCapabilities(existingPods, existingNodes);

        AtomicReference<Triplet<Pod, Node, Double>> nodePodWithHighestScore = new AtomicReference<>(new Triplet<>(null, null, 100.0));

        unscheduledPods.getItems().forEach(pod -> {

            TreeMap<NodeWithAlloc, Double> singleNodePodScore = K8Helper.calculateNodeScore(nodesWithAllocList, pod);
            if (!singleNodePodScore.isEmpty()) {
                var singleNodePodMaxScore = Collections.min(singleNodePodScore.entrySet(), Comparator.comparingDouble(Map.Entry::getValue));

                if (singleNodePodMaxScore.getValue() < nodePodWithHighestScore.get().getValue2()) {
                    nodePodWithHighestScore.set(new Triplet<>(pod, singleNodePodMaxScore.getKey().getNode(), singleNodePodMaxScore.getValue()));
                    System.out.println("Finally schedule FIFO Queue with the queue: ");
                    Pair<Pod, Node> pair = K8Helper.bindPodToNode(nodePodWithHighestScore.get().getValue0(), nodePodWithHighestScore.get().getValue1(), nodePodWithHighestScore.get().getValue2());
                    unscheduledPods.getItems().remove(nodePodWithHighestScore.get().getValue0());
                    Pod podToAdd = pair.getValue0();
                    podToAdd.getSpec().setNodeName(pair.getValue1().getMetadata().getName());
                    PodNodeScheduler.podList.addPodToList(podToAdd);
                    return;
                }
            }
        });

    }

}
