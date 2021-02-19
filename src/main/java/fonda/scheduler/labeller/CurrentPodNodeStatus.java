package fonda.scheduler.labeller;

import fonda.scheduler.distributedscheduler.PodNodeScheduler;
import fonda.scheduler.model.PodListWithIndex;
import io.fabric8.kubernetes.api.model.*;
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

import java.util.Collections;
import java.util.concurrent.ConcurrentLinkedQueue;

public class CurrentPodNodeStatus {

    private KubernetesClient client;

    private OperationContext operationContext;

    private ConcurrentLinkedQueue<SharedInformerEventListener> workerQueueNode;

    DefaultSharedIndexInformer<Node, NodeList> defaultSharedIndexInformerNode;

    public CurrentPodNodeStatus(KubernetesClient client) {
        this.client = client;

        PodNodeScheduler.podList = new PodListWithIndex();

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

        client.pods().watch(new Watcher<Pod>() {
            @Override
            public void eventReceived(Action action, Pod pod) {


                switch (action) {
                    case ADDED:
                        PodNodeScheduler.podList.addPodToList(pod);

                        if(PodNodeScheduler.unscheduledPods.getItems().size()>0) {
                            PodNodeScheduler.unscheduledPods.getItems().add(pod);
                            break;
                        }

                        if (pod.getSpec().getNodeName() == null) {
                            Pair<Pod, Node> scheduledPair = PodNodeScheduler.scheduleRR(pod, "").orElse(new Pair<>(pod, null)); // change scheduling approach here
                            pod.getSpec().setNodeName(scheduledPair.getValue1().getMetadata().getName());
                            PodNodeScheduler.podList.addPodToList(pod);
                        }
                        break;
                    case MODIFIED:
                        break;
                    case DELETED:
                        PodNodeScheduler.podList.removePodFromList(pod);
                        PodNodeScheduler.scheduleRR( null, "");
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
                PodNodeScheduler.nodeList = client.nodes().list();
                Collections.shuffle(PodNodeScheduler.nodeList.getItems()); // use this for scheduling experiments
                return PodNodeScheduler.nodeList;
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
