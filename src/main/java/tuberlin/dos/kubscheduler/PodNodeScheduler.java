package tuberlin.dos.kubscheduler;

import io.fabric8.kubernetes.api.model.*;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class PodNodeScheduler {

    public static NodeList nodeList;
    public static PodListWithIndex podList;
    public static PodList unscheduledPods = new PodList();

    private PodNodeScheduler() {

    }

    static synchronized Optional<Pair<Pod, Node>> schedule(Pod podToSchedule, String str) {
        if (podToSchedule != null) {
            return schedule(podList, nodeList, podToSchedule);
        } else {
            scheduleQueue(podList, nodeList);
            return Optional.empty();
        }
    }


    static Optional<Pair<Pod, Node>> schedule(PodList existingPods, NodeList existingNodes, Pod podToSchedule) {

        List<NodeWithAlloc> nodesWithAllocList = estimateFreeNodeCapabilities(existingPods, existingNodes);

        TreeMap<NodeWithAlloc, Integer> nodeScore = calculateNodeScore(nodesWithAllocList, podToSchedule);

        if (nodeScore.isEmpty()) {
            System.out.println("Pod " + podToSchedule.getMetadata().getName() + " was unable to get scheduled due to insufficient resources. Added to unscheduled queue");
            unscheduledPods.getItems().add(podToSchedule);
            return Optional.empty();
        } else {
            NodeWithAlloc maxNode = Collections.max(nodeScore.entrySet(), Comparator.comparingInt(Map.Entry::getValue)).getKey();
            //raise everyone else's age
            unscheduledPods.getItems().forEach(podWithAge -> ((PodWithAge) podWithAge).setAge(((PodWithAge) podWithAge).getAge().add(BigDecimal.ONE)));
            return Optional.of(bindPodToNode(podToSchedule, maxNode.getNode(), nodeScore.get(maxNode)));

        }

    }

    static void scheduleQueue(PodList existingPods, NodeList existingNodes) {

        if (unscheduledPods.getItems().isEmpty()) {
            return;
        }
        if (getStarvingPod().isPresent()) {
            schedule(existingPods, existingNodes, getStarvingPod().get());
        } else {
            List<NodeWithAlloc> nodesWithAllocList = estimateFreeNodeCapabilities(existingPods, existingNodes);

            AtomicReference<Triplet<Pod, Node, Integer>> nodePodWithHighestScore = new AtomicReference<>(new Triplet<>(null, null, -1));

            unscheduledPods.getItems().forEach(pod -> {

                TreeMap<NodeWithAlloc, Integer> singleNodePodScore = calculateNodeScore(nodesWithAllocList, pod);
                if (!singleNodePodScore.isEmpty()) {
                    var singleNodePodMaxScore = Collections.max(singleNodePodScore.entrySet(), Comparator.comparingInt(Map.Entry::getValue));

                    if (singleNodePodMaxScore.getValue() > nodePodWithHighestScore.get().getValue2()) {
                        nodePodWithHighestScore.set(new Triplet<>(pod, singleNodePodMaxScore.getKey().getNode(), singleNodePodMaxScore.getValue()));


                    }
                }
            });

            if (nodePodWithHighestScore.get().getValue2() > -1) {
                Pair<Pod, Node> pair = bindPodToNode(nodePodWithHighestScore.get().getValue0(), nodePodWithHighestScore.get().getValue1(), nodePodWithHighestScore.get().getValue2());
                unscheduledPods.getItems().remove(nodePodWithHighestScore.get().getValue0());
                //raise everyone else's age
                unscheduledPods.getItems().forEach(podWithAge -> ((PodWithAge) podWithAge).setAge(((PodWithAge) podWithAge).getAge().add(BigDecimal.ONE)));

                Pod podToAdd = pair.getValue0();
                podToAdd.getSpec().setNodeName(pair.getValue1().getMetadata().getName());
                PodNodeScheduler.podList.addPodToList(podToAdd);
            }

        }

    }

    public static Optional<Pod> getStarvingPod() {
        BigDecimal maxAge = BigDecimal.ZERO;
        Pod oldestPod = unscheduledPods.getItems().get(0);
        BigDecimal averageAge = BigDecimal.ZERO;
        BigDecimal stdDev = BigDecimal.ZERO;
        for (Pod pod : unscheduledPods.getItems()) {
            PodWithAge pwa = (PodWithAge) pod;
            if (pwa.getAge().compareTo(maxAge) > 0) {
                maxAge = pwa.getAge();
                oldestPod = pwa;
            }
           averageAge = averageAge.add(pwa.getAge());
        }
        averageAge = averageAge.divide(BigDecimal.valueOf(unscheduledPods.getItems().size()), RoundingMode.CEILING);
        for (Pod pod : unscheduledPods.getItems()) {
            PodWithAge pwa = (PodWithAge) pod;
            stdDev = stdDev.add(pwa.getAge().subtract(averageAge).pow(2));


        }
        stdDev = stdDev.divide(BigDecimal.valueOf(unscheduledPods.getItems().size()).subtract(BigDecimal.ONE), RoundingMode.CEILING);
        stdDev = stdDev.sqrt(new MathContext(2));
        //If age of oldest is more than a stddev bigger than aberage, then the pod is starving
        if (maxAge.compareTo(averageAge.add(stdDev)) > 0) {
            System.out.println("Max age " + maxAge + " avg age " + averageAge + ". Starving pod is " + oldestPod.getMetadata().getName());
            return Optional.of(oldestPod);
        }
        return Optional.empty();
    }

    static List<NodeWithAlloc> estimateFreeNodeCapabilities(PodList existingPods, NodeList existingNodes) {
        TreeMap<NodeWithAlloc, Integer> nodeScore = new TreeMap<>();

        List<NodeWithAlloc> nodeWithAllocList = new ArrayList<>();

        for (Node node : existingNodes.getItems()) {

            NodeWithAlloc nodeWithAlloc = new NodeWithAlloc(node);
            nodeWithAlloc.setNode(node);


            for (Pod pod : existingPods.getItems()) {

                if (node.getMetadata().getName().equals(pod.getSpec().getNodeName())) {

                    if (pod.getSpec().getContainers().get(0).getResources().getRequests() != null && pod.getSpec().getContainers().get(0).getResources().getRequests().get("cpu") != null) {
                        Quantity pod_cpu = pod.getSpec().getContainers().get(0).getResources().getRequests().get("cpu"); // hier CPU und memory raus ziehen, anschließend auf noch die Methode zum umrechnen der Einheiten benutzen.
                        nodeWithAlloc.setCurrent_cpu_usage(nodeWithAlloc.getCurrent_cpu_usage().add(Quantity.getAmountInBytes(pod_cpu)));
                    }
                    if (pod.getSpec().getContainers().get(0).getResources().getRequests() != null && pod.getSpec().getContainers().get(0).getResources().getRequests().get("memory") != null) {
                        Quantity pod_memory = pod.getSpec().getContainers().get(0).getResources().getRequests().get("memory");
                        nodeWithAlloc.setCurrent_ram_usage(nodeWithAlloc.getCurrent_ram_usage().add(Quantity.getAmountInBytes(pod_memory)));
                    }


                }

            }
            // Methode berechnet, wie viele bytes noch frei sind
            nodeWithAlloc.calculateAlloc();
            nodeWithAllocList.add(nodeWithAlloc);
        }

        return nodeWithAllocList;
    }

    static TreeMap<NodeWithAlloc, Integer> calculateNodeScore(List<NodeWithAlloc> nodesWithAllocList, Pod podToSchedule) {

        TreeMap<NodeWithAlloc, Integer> nodeScore = new TreeMap<>();

        nodesWithAllocList.forEach(node -> {

            // Hat die Node ausreichend CPU ind Speicher, um den Pod zu schedulen

            if (node.getFree_cpu().compareTo(Quantity.getAmountInBytes(podToSchedule.getSpec().getContainers().get(0).getResources().getLimits().get("cpu"))) >= 0 &&
                    node.getFree_ram().compareTo(Quantity.getAmountInBytes(podToSchedule.getSpec().getContainers().get(0).getResources().getLimits().get("memory"))) >= 0) {

                // Berechne den Score, gehe dabei alle Labels der Node und die preferredDuringSchedulingIgnoredDuringExecution der Pods durch
                node.getNode().getMetadata().getLabels().forEach((key, val) -> {

                    podToSchedule.getSpec().getAffinity().getNodeAffinity().getPreferredDuringSchedulingIgnoredDuringExecution().forEach(preferredSchedulingTerm -> {
                        if (preferredSchedulingTerm.getPreference().getMatchExpressions().get(0).getKey().equalsIgnoreCase(key) &&
                                preferredSchedulingTerm.getPreference().getMatchExpressions().get(0).getValues().get(0).equalsIgnoreCase(val)) {

                            if (nodeScore.containsKey(node)) {
                                nodeScore.put(node, nodeScore.get(node) + preferredSchedulingTerm.getWeight());
                            } else {
                                nodeScore.put(node, preferredSchedulingTerm.getWeight());
                            }

                        }
                    });
                    // Für den Fall das keine Labels matchen aber die Node frei ist. Der score ist dann 0
                    if (!nodeScore.containsKey(node)) {
                        nodeScore.put(node, 0);
                    }

                });

            }
        });

        return nodeScore;

    }

    static Pair<Pod, Node> bindPodToNode(Pod pod, Node node, Integer score) {
        Binding b1 = new Binding();

        ObjectMeta om = new ObjectMeta();
        om.setName(pod.getMetadata().getName());
        om.setNamespace(pod.getMetadata().getNamespace());
        b1.setMetadata(om);

        ObjectReference objectReference = new ObjectReference();
        objectReference.setApiVersion("v1");
        objectReference.setKind("Node");
        objectReference.setName(node.getMetadata().getName()); //hier ersetzen durch tatsächliche Node

        b1.setTarget(objectReference);

        try {
            System.out.println("Bind " + pod.getMetadata().getName() + " to " + node.getMetadata().getName());
            var bind = KubernetesClientSingleton.getKubernetesClient().bindings().create(b1);
            //  TaskDB.addSchedulingReportToDB(pod, node, score);
            return new Pair<Pod, Node>(pod, node);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }


    }


}
