package fonda.scheduler.distributedscheduler;

import fonda.scheduler.controller.KubernetesClientSingleton;
import fonda.scheduler.model.NodeWithAlloc;
import io.fabric8.kubernetes.api.model.*;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class K8Helper {

    private static final Logger logger = LoggerFactory.getLogger(K8Helper.class);

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
                        nodeScore.put(node, 3.0);
                    }

                });

            }
        });
        return nodeScore;

    }

    static NodeWithAlloc getMostPowerfulNode(List<NodeWithAlloc> nodesWithAllocList) {

        if (nodesWithAllocList == null) {
            return null;
        }

        AtomicReference<NodeWithAlloc> mostPowerfulNode = new AtomicReference<>();
        AtomicReference<Double> mostPNValue = new AtomicReference<>(0.0);

        nodesWithAllocList.forEach(node -> {

            AtomicReference<Double> nodeValue = new AtomicReference<>(0.0);

            node.getNode().getMetadata().getLabels().forEach((key, val) -> {
                if (key.equalsIgnoreCase("CPU_ST") || key.equalsIgnoreCase("RAM")) {
                    nodeValue.updateAndGet(v -> (double) (v + Integer.valueOf(val)));
                }
            });

            if (mostPowerfulNode.get() == null) {
                mostPNValue.updateAndGet(mpnv -> mpnv = nodeValue.get());
                mostPowerfulNode.updateAndGet(v -> v = node);
            } else if (mostPNValue.get() < nodeValue.get()) {
                mostPNValue.updateAndGet(mpnv -> mpnv = nodeValue.get());
                mostPowerfulNode.updateAndGet(v -> v = node);
            }

        });
        System.out.println("Most powerful node has value: " + mostPNValue);
        return mostPowerfulNode.get();
    }

    static String getFullPodName(Pod pod) {
        return pod.getMetadata().getLabels().get("taskName");
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
        objectReference.setName(node.getMetadata().getName());

        b1.setTarget(objectReference);

        try {
            logger.info("[" + pod.getSpec().getContainers().get(0).getName() + "] - " + "Bind " + getFullPodName(pod) + " to " + node.getMetadata().getName() + " with score: " + score);
            var bind = KubernetesClientSingleton.getKubernetesClient().bindings().create(b1);
            return new Pair<Pod, Node>(pod, node);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }


    }

    static Pair<NodeWithAlloc, Double> findOptimalNodeWithAlloc(TreeMap<NodeWithAlloc, Double> nodeScore) {

        List<NodeWithAlloc> maxNodes = new ArrayList<>();

        var value = Collections.min(nodeScore.entrySet(), Comparator.comparingDouble(Map.Entry::getValue)).getValue();

        nodeScore.forEach((key, val) -> {
            if (val.equals(value)) {
                maxNodes.add(key);
            }
        });

        return new Pair<>(Collections.max(maxNodes, Comparator.comparingDouble(NodeWithAlloc::getCurrent_cpu_as_Double).reversed()), value);

    }
}
