package tuberlin.dos.kubscheduler;

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

import java.util.concurrent.ConcurrentLinkedQueue;

public class CurrentPodNodeStatus {

    private KubernetesClient client;

    private OperationContext operationContext;

    private ConcurrentLinkedQueue<SharedInformerEventListener> workerQueueNode;

    private ConcurrentLinkedQueue<SharedInformerEventListener> workerQueuePod;

    DefaultSharedIndexInformer<Node, NodeList> defaultSharedIndexInformerNode;

    DefaultSharedIndexInformer<Pod, PodList> defaultSharedIndexInformerPod;

    private NodeList nodeList;

    private PodListWithIndex podList;

    public CurrentPodNodeStatus(KubernetesClient client) {
        this.client = client;

        this.podList = new PodListWithIndex();

        this.workerQueueNode = new ConcurrentLinkedQueue<>();
        this.workerQueuePod = new ConcurrentLinkedQueue<>();

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
                        podList.addPodToList(pod);
                        if (pod.getSpec().getNodeName() == null) {
                            Pair<Pod, Node> scheduledPair = PodNodeScheduler.schedule(podList, nodeList, pod).orElse(new Pair<>(pod, null));
                            pod.getSpec().setNodeName(scheduledPair.getValue1().getMetadata().getName()); // Hier stimmt evtl etwas nicht
                            podList.addPodToList(pod);
                        }
                        break;
                    case MODIFIED:
                        break;
                    case DELETED:
                        podList.removePodFromList(pod); //klappt das?
                        PodNodeScheduler.scheduleQueue(podList, nodeList);
                }

                System.out.println(podList.getItems());
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
                nodeList = client.nodes().list();
                return nodeList;
            }
        };

        defaultSharedIndexInformerNode = new DefaultSharedIndexInformer(Node.class, listerWatcher, 60000, operationContext, workerQueueNode);

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

    /**
     *
     */
    private void setUpIndexInformerPod() {
        ListerWatcher listerWatcher = new ListerWatcher() {
            @Override
            public Watch watch(ListOptions params, String namespace, OperationContext context, Watcher watcher) {

                params.setFieldSelector("spec.schedulerName=new-scheduler");

                return client.pods().watch(params, new Watcher<Pod>() {
                    @Override
                    public void eventReceived(Action action, Pod resource) {
                        if (action.name().equals("ADDED")) {

                        }
                    }

                    @Override
                    public void onClose(WatcherException cause) {
                        System.out.println("On close");
                    }
                });
            }

            @Override
            public Object list(ListOptions params, String namespace, OperationContext context) {

                params.setFieldSelector("spec.schedulerName=new-scheduler");
               /* PodList podListToReturn = null;

                try {
                    podListToReturn = client.pods().list();

                    var filteredPodList = podListToReturn.getItems().stream().filter(pod -> {
                        if (pod.getSpec().getNodeName() == null && pod.getSpec().getSchedulerName().equalsIgnoreCase("new-scheduler")) {
                            return true;
                        } else {
                            return false;
                        }
                    }).collect(Collectors.toList());


                    podList.setItems(podListToReturn.getItems());
                    podListToReturn.setItems(filteredPodList);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                return podListToReturn;
                */
                return client.pods().list(params);
            }
        };

        defaultSharedIndexInformerPod = new DefaultSharedIndexInformer(Pod.class, listerWatcher, 5000, operationContext, workerQueuePod);

        try {
            defaultSharedIndexInformerPod.run();
        } catch (Exception e) {
            e.printStackTrace();
        }

        defaultSharedIndexInformerPod.addEventHandler(new ResourceEventHandler<Pod>() {
            @Override
            public void onAdd(Pod obj) {
                if (obj.getSpec().getNodeName() == null) {
                    PodNodeScheduler.schedule(podList, nodeList, obj);
                }

            }

            @Override
            public void onUpdate(Pod oldObj, Pod newObj) {
            }

            @Override
            public void onDelete(Pod obj, boolean deletedFinalStateUnknown) {
                System.out.println("pod got deleted " + obj.getMetadata().getName() + " with boolean: " + deletedFinalStateUnknown);
                PodNodeScheduler.scheduleQueue(podList, nodeList);
            }
        });
    }

}
