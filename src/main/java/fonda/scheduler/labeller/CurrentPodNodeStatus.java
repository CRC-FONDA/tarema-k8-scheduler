package fonda.scheduler.labeller;

import fonda.scheduler.distributedscheduler.SJFNScheduler;
import fonda.scheduler.model.PodListWithIndex;
import io.fabric8.kubernetes.api.model.ListOptions;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeList;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.fabric8.kubernetes.client.dsl.base.OperationContext;
import io.fabric8.kubernetes.client.informers.ListerWatcher;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedInformerEventListener;
import io.fabric8.kubernetes.client.informers.impl.DefaultSharedIndexInformer;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.ConcurrentLinkedQueue;

public class CurrentPodNodeStatus {

    private KubernetesClient client;

    private OperationContext operationContext;

    private ConcurrentLinkedQueue<SharedInformerEventListener> workerQueueNode;

    DefaultSharedIndexInformer<Node, NodeList> defaultSharedIndexInformerNode;

    private static final Logger logger = LoggerFactory.getLogger(CurrentPodNodeStatus.class);

    public CurrentPodNodeStatus(KubernetesClient client) {
        this.client = client;

        SJFNScheduler.podList = new PodListWithIndex();

        this.workerQueueNode = new ConcurrentLinkedQueue<>();

        this.operationContext = new OperationContext();


        setUpIndexInformerNode();

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // setUpIndexInformerPod();

        ListOptions options = new ListOptions();
        options.setFieldSelector("spec.schedulerName=new-scheduler");

        client.pods().watch(options, new Watcher<Pod>() {
            @Override
            public void eventReceived(Action action, Pod pod) {


                switch (action) {
                    case ADDED:
                        logger.info("[" + pod.getSpec().getContainers().get(0).getName() + "] - " + "New Pod added to Scheduler: " + pod.getMetadata().getName());

                        pod.getSpec().getAffinity().getNodeAffinity().getPreferredDuringSchedulingIgnoredDuringExecution().forEach(preferredSchedulingTerm -> {
                            if (preferredSchedulingTerm.getPreference().getMatchExpressions().get(0).getKey().equalsIgnoreCase("RAM") || preferredSchedulingTerm.getPreference().getMatchExpressions().get(0).getKey().equalsIgnoreCase("CPU_ST")) {
                                    System.out.println("Key: " + preferredSchedulingTerm.getPreference().getMatchExpressions().get(0).getKey() + ": " + Integer.valueOf(preferredSchedulingTerm.getPreference().getMatchExpressions().get(0).getValues().get(0)));
                            }
                        });

                        SJFNScheduler.podList.addPodToList(pod);

                        if (SJFNScheduler.unscheduledPods.getItems().size() > 0) {
                            SJFNScheduler.unscheduledPods.getItems().add(pod);
                            break;
                        }

                        if (pod.getSpec().getNodeName() == null) {
                            Pair<Pod, Node> scheduledPair = SJFNScheduler.scheduleSJFN(pod).orElse(new Pair<>(pod, null)); // change scheduling approach here
                            if (scheduledPair.getValue1() == null) {
                                pod.getSpec().setNodeName(null);
                            } else {
                                pod.getSpec().setNodeName(scheduledPair.getValue1().getMetadata().getName());
                            }
                            SJFNScheduler.podList.addPodToList(pod);
                        }
                        break;
                    case MODIFIED:
                        break;
                    case DELETED:
                        logger.info("[" + pod.getSpec().getContainers().get(0).getName() + "] - " + "Pod deleted: " + pod.getMetadata().getName());
                        SJFNScheduler.podList.removePodFromList(pod);
                        SJFNScheduler.scheduleSJFN(null);
                        
                }

            }

            @Override
            public void onClose(WatcherException cause) {

            }
        });


    }

    /**
     *
     */
    private void setUpIndexInformerNode() {
        ListerWatcher listerWatcher = new ListerWatcher() {
            @Override
            public Watch watch(ListOptions params, String namespace, OperationContext context, Watcher watcher) {
                return client.nodes().watch(new Watcher<Node>() {
                    @Override
                    public void eventReceived(Action action, Node resource) {

                    }

                    @Override
                    public void onClose(WatcherException cause) {

                    }
                });
            }

            @Override
            public Object list(ListOptions params, String namespace, OperationContext context) {
                SJFNScheduler.nodeList = client.nodes().list();
                Collections.shuffle(SJFNScheduler.nodeList.getItems()); // use this for scheduling experiments
                return SJFNScheduler.nodeList;
            }
        };

        defaultSharedIndexInformerNode = new DefaultSharedIndexInformer(Node.class, listerWatcher, 24000, operationContext, workerQueueNode);

        try {
            defaultSharedIndexInformerNode.run();
        } catch (Exception e) {
            e.printStackTrace();
        }

        defaultSharedIndexInformerNode.addEventHandler(new ResourceEventHandler<Node>() {
            @Override
            public void onAdd(Node obj) {

            }

            @Override
            public void onUpdate(Node oldObj, Node newObj) {

            }

            @Override
            public void onDelete(Node obj, boolean deletedFinalStateUnknown) {

            }
        });
    }

}
