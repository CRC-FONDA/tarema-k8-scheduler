package tuberlin.dos.kubscheduler;

import io.fabric8.kubernetes.api.model.*;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class PodNodeScheduler {

    private static PodList unscheduledPods = new PodList();

    private PodNodeScheduler() {

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

            return Optional.of(bindPodToNode(podToSchedule, maxNode.getNode(), nodeScore.get(maxNode)));

        }

    }

    static void scheduleQueue(PodList existingPods, NodeList existingNodes) {

        if (unscheduledPods.getItems().isEmpty()) {
            return;
        }

        List<NodeWithAlloc> nodesWithAllocList = estimateFreeNodeCapabilities(existingPods, existingNodes);
        // nodeswithalloc ist hier falsch. Wahrscheinlich wurde etwas nicht rausgelöscht. Noch mal starten
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
            bindPodToNode(nodePodWithHighestScore.get().getValue0(), nodePodWithHighestScore.get().getValue1(), nodePodWithHighestScore.get().getValue2());
        }

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
            var bind = KubernetesClientSingleton.getKubernetesClient().bindings().create(b1);
          //  TaskDB.addSchedulingReportToDB(pod, node, score);
            return new Pair<Pod, Node>(pod, node);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }


    }


}
