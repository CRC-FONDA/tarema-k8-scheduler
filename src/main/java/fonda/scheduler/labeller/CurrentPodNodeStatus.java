package fonda.scheduler.labeller;

import fonda.scheduler.distributedscheduler.PodNodeScheduler;
import fonda.scheduler.model.PodListWithIndex;
import fonda.scheduler.model.PodWithAge;
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

import java.util.concurrent.ConcurrentLinkedQueue;

public class CurrentPodNodeStatus {

    DefaultSharedIndexInformer<Node, NodeList> defaultSharedIndexInformerNode;
    private KubernetesClient client;
    private OperationContext operationContext;
    private ConcurrentLinkedQueue<SharedInformerEventListener> workerQueueNode;

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

        client.pods().watch(new Watcher<>() {
            @Override
            public void eventReceived(Action action, Pod pod) {

                PodWithAge pwa = new PodWithAge(pod);
                switch (action) {
                    case ADDED:
                        PodNodeScheduler.podList.addPodToList(pwa);
                        if (pwa.getSpec().getNodeName() == null) {
                            Pair<Pod, Node> scheduledPair = PodNodeScheduler.schedule(pwa, "").orElse(new Pair<>(pwa, null));
                            pwa.getSpec().setNodeName(scheduledPair.getValue1().getMetadata().getName()); // Hier stimmt evtl etwas nicht
                            PodNodeScheduler.podList.addPodToList(pwa);
                        }
                        break;
                    case MODIFIED:
                        break;
                    case DELETED:
                        PodNodeScheduler.podList.removePodFromList(pwa); //klappt das?
                        PodNodeScheduler.schedule(null, "");
                }

                //  System.out.println("Currently unscheduled pods: ");
                //  PodNodeScheduler.unscheduledPods.getItems().forEach(podItem ->System.out.println(podItem.getMetadata().getName() + " age " + ((PodWithAge)podItem).getAge()) );

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
                return client.nodes().watch(new Watcher<>() {
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
                return PodNodeScheduler.nodeList;
            }
        };

        defaultSharedIndexInformerNode = new DefaultSharedIndexInformer(Node.class, listerWatcher, 600000, operationContext, workerQueueNode);

        try {
            defaultSharedIndexInformerNode.run();
        } catch (Exception e) {
            e.printStackTrace();
        }

        defaultSharedIndexInformerNode.addEventHandler(new ResourceEventHandler<>() {
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
