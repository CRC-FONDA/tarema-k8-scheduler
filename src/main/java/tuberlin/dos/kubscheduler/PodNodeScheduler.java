package tuberlin.dos.kubscheduler;

import io.fabric8.kubernetes.api.model.*;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class PodNodeScheduler {

    public static PodList unscheduledPods = new PodList();

    public static NodeList nodeList;

    public static PodListWithIndex podList;

    private static int nodeIndex = 0;

    private PodNodeScheduler() {

    }

    static synchronized Optional<Pair<Pod, Node>> schedule(Pod podToSchedule, String str) {
        if (podToSchedule != null) {
            return schedule(podList, nodeList, podToSchedule);
        } else {
            System.out.println("schedule Queue with size: " + unscheduledPods.getItems().size());
            scheduleQueue(podList, nodeList);
            return Optional.empty();
        }
    }

    static synchronized Optional<Pair<Pod, Node>> scheduleRR(Pod podToSchedule, String str) {
        if (podToSchedule != null) {
            return scheduleRR(podList, nodeList, podToSchedule, 0);
        } else {
            scheduleQueueFIFO(podList, nodeList);
            return Optional.empty();
        }
    }


    static Optional<Pair<Pod, Node>> schedule(PodList existingPods, NodeList existingNodes, Pod podToSchedule) {

        List<NodeWithAlloc> nodesWithAllocList = estimateFreeNodeCapabilities(existingPods, existingNodes);

        TreeMap<NodeWithAlloc, Double> nodeScore = calculateNodeScore(nodesWithAllocList, podToSchedule);

        if (nodeScore.isEmpty()) {
            System.out.println("Pod " + podToSchedule.getMetadata().getLabels().get("taskName") + " was unable to get scheduled due to insufficient resources. Added to unscheduled queue");
            unscheduledPods.getItems().add(podToSchedule);
            return Optional.empty();
        } else {
            //with fair scheduling -> default for Tarema and the fair scheduler
            var pair = findOptimalNodeWithAlloc(nodeScore);

            NodeWithAlloc maxNode = pair.getValue0();

            // without fair scheduling -> fill nodes approach
            //NodeWithAlloc maxNode =  Collections.min(nodeScore.entrySet(), Comparator.comparingInt(Map.Entry::getValue)).getKey();

            return Optional.of(bindPodToNode(podToSchedule, maxNode.getNode(), pair.getValue1()));

        }

    }

    private static Pair<NodeWithAlloc, Double> findOptimalNodeWithAlloc(TreeMap<NodeWithAlloc, Double> nodeScore) {

        List<NodeWithAlloc> maxNodes = new ArrayList<>();

        var value = Collections.min(nodeScore.entrySet(), Comparator.comparingDouble(Map.Entry::getValue)).getValue();

        nodeScore.forEach((key, val) -> {
            if (val.equals(value)) {
                maxNodes.add(key);
            }
        });

        return new Pair<>(Collections.min(maxNodes, Comparator.comparingDouble(NodeWithAlloc::getCurrent_cpu_as_Double).reversed()), value);

    }

    static Optional<Pair<Pod, Node>> scheduleRR(PodList existingPods, NodeList existingNodes, Pod podToSchedule, int counter) {

        if (counter == existingNodes.getItems().size()) {
            unscheduledPods.getItems().add(podToSchedule);
            return Optional.empty();
        }

        if (nodeIndex >= existingNodes.getItems().size()) {
            nodeIndex = 0;
        }

        List<NodeWithAlloc> nodesWithAllocList = estimateFreeNodeCapabilities(existingPods, existingNodes.getItems().get(nodeIndex));

        TreeMap<NodeWithAlloc, Double> nodeScore = calculateNodeScore(nodesWithAllocList, podToSchedule);

        if (nodeScore.isEmpty()) {
            nodeIndex++;
            return scheduleRR(existingPods, existingNodes, podToSchedule, counter + 1);
        } else {
            nodeIndex++;
            return Optional.of(bindPodToNode(podToSchedule, nodesWithAllocList.get(0).getNode(), -1.0));
        }

    }

    static void scheduleQueue(PodList existingPods, NodeList existingNodes) {

        if (unscheduledPods.getItems().isEmpty()) {
            return;
        }

        List<NodeWithAlloc> nodesWithAllocList = estimateFreeNodeCapabilities(existingPods, existingNodes);

        AtomicReference<Triplet<Pod, Node, Double>> nodePodWithHighestScore = new AtomicReference<>(new Triplet<>(null, null, 10.0));

        unscheduledPods.getItems().forEach(pod -> {

            TreeMap<NodeWithAlloc, Double> singleNodePodScore = calculateNodeScore(nodesWithAllocList, pod);
            if (!singleNodePodScore.isEmpty()) {
                var singleNodePodMaxScore = Collections.min(singleNodePodScore.entrySet(), Comparator.comparingDouble(Map.Entry::getValue));

                if (singleNodePodMaxScore.getValue() < nodePodWithHighestScore.get().getValue2()) {
                    nodePodWithHighestScore.set(new Triplet<>(pod, singleNodePodMaxScore.getKey().getNode(), singleNodePodMaxScore.getValue()));


                }
            }
        });

        if (nodePodWithHighestScore.get().getValue2() < 10.0) {
            System.out.println("Finally schedule Queue with the queue: ");
            Pair<Pod, Node> pair = bindPodToNode(nodePodWithHighestScore.get().getValue0(), nodePodWithHighestScore.get().getValue1(), nodePodWithHighestScore.get().getValue2());
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

        List<NodeWithAlloc> nodesWithAllocList = estimateFreeNodeCapabilities(existingPods, existingNodes);

        AtomicReference<Triplet<Pod, Node, Double>> nodePodWithHighestScore = new AtomicReference<>(new Triplet<>(null, null, 10.0));

        unscheduledPods.getItems().forEach(pod -> {

            TreeMap<NodeWithAlloc, Double> singleNodePodScore = calculateNodeScore(nodesWithAllocList, pod);
            if (!singleNodePodScore.isEmpty()) {
                var singleNodePodMaxScore = Collections.min(singleNodePodScore.entrySet(), Comparator.comparingDouble(Map.Entry::getValue));

                if (singleNodePodMaxScore.getValue() < nodePodWithHighestScore.get().getValue2()) {
                    nodePodWithHighestScore.set(new Triplet<>(pod, singleNodePodMaxScore.getKey().getNode(), singleNodePodMaxScore.getValue()));
                    System.out.println("Finally schedule FIFO Queue with the queue: ");
                    Pair<Pod, Node> pair = bindPodToNode(nodePodWithHighestScore.get().getValue0(), nodePodWithHighestScore.get().getValue1(), nodePodWithHighestScore.get().getValue2());
                    unscheduledPods.getItems().remove(nodePodWithHighestScore.get().getValue0());
                    Pod podToAdd = pair.getValue0();
                    podToAdd.getSpec().setNodeName(pair.getValue1().getMetadata().getName());
                    PodNodeScheduler.podList.addPodToList(podToAdd);
                    return;
                }
            }
        });

    }


    static List<NodeWithAlloc> estimateFreeNodeCapabilities(PodList existingPods, NodeList existingNodes) {

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

    static List<NodeWithAlloc> estimateFreeNodeCapabilities(PodList existingPods, Node existingNode) {


        List<NodeWithAlloc> nodeWithAllocList = new ArrayList<>();


        NodeWithAlloc nodeWithAlloc = new NodeWithAlloc(existingNode);
        nodeWithAlloc.setNode(existingNode);

        for (Pod pod : existingPods.getItems()) {


            if (existingNode.getMetadata().getName().equals(pod.getSpec().getNodeName())) {
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


        return nodeWithAllocList;
    }

    static TreeMap<NodeWithAlloc, Double> calculateNodeScore(List<NodeWithAlloc> nodesWithAllocList, Pod podToSchedule) {

        TreeMap<NodeWithAlloc, Double> nodeScore = new TreeMap<>();

        nodesWithAllocList.forEach(node -> {

            // Hat die Node ausreichend CPU ind Speicher, um den Pod zu schedulen

            if (node.getFree_cpu().compareTo(Quantity.getAmountInBytes(podToSchedule.getSpec().getContainers().get(0).getResources().getLimits().get("cpu"))) >= 0 &&
                    node.getFree_ram().compareTo(Quantity.getAmountInBytes(podToSchedule.getSpec().getContainers().get(0).getResources().getLimits().get("memory"))) >= 0) {

                // Berechne den Score, gehe dabei alle Labels der Node und die preferredDuringSchedulingIgnoredDuringExecution der Pods durch
                node.getNode().getMetadata().getLabels().forEach((key, val) -> {

                    podToSchedule.getSpec().getAffinity().getNodeAffinity().getPreferredDuringSchedulingIgnoredDuringExecution().forEach(preferredSchedulingTerm -> {
                        if (preferredSchedulingTerm.getPreference().getMatchExpressions().get(0).getKey().equalsIgnoreCase(key)) {

                            if (nodeScore.containsKey(node)) {
                                nodeScore.put(node, nodeScore.get(node) + (Math.abs(Integer.valueOf(val) - Integer.valueOf(preferredSchedulingTerm.getPreference().getMatchExpressions().get(0).getValues().get(0)))) * getWeight(preferredSchedulingTerm.getPreference().getMatchExpressions().get(0).getValues().get(0)));
                            } else {
                                nodeScore.put(node, (Math.abs(Integer.valueOf(val) - Integer.valueOf(preferredSchedulingTerm.getPreference().getMatchExpressions().get(0).getValues().get(0)))) * getWeight(preferredSchedulingTerm.getPreference().getMatchExpressions().get(0).getValues().get(0)));
                            }

                        }
                    });
                    // Für den Fall das keine Labels matchen aber die Node frei ist. Der score ist dann 0
                    if (!nodeScore.containsKey(node)) {
                        nodeScore.put(node, 10.0);
                    }

                });

            }
        });
        return nodeScore;

    }

    private static Double getWeight(String str) {
        if (Integer.valueOf(str).equals(3)) {
            return 1.1;
        } else if (Integer.valueOf(str).equals(2)) {
            return 1.05;
        } else {
            return 1.0;
        }
    }

    static Pair<Pod, Node> bindPodToNode(Pod pod, Node node, Double score) {
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
            System.out.println("Bind " + pod.getMetadata().getLabels().get("taskName") + " to" + node.getMetadata().getName() + " with score: " + score);
            var bind = KubernetesClientSingleton.getKubernetesClient().bindings().create(b1);
            //  TaskDB.addSchedulingReportToDB(pod, node, score);
            return new Pair<Pod, Node>(pod, node);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }


    }


}
