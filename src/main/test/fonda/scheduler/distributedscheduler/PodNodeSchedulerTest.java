package fonda.scheduler.distributedscheduler;

import fonda.scheduler.controller.KubernetesClientSingleton;
import fonda.scheduler.model.PodListWithIndex;
import fonda.scheduler.model.PodWithAge;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.KubernetesClient;
import junit.framework.TestCase;
import org.junit.Assert;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Optional;

import static org.mockito.Mockito.mock;

public class PodNodeSchedulerTest extends TestCase {

    //TODO: use PowerMock
    public void testScheduleQueue() {
        PodWithAge pod1 = new PodWithAge(new ObjectMeta(), new PodSpec(), new PodStatus());
        pod1.getMetadata().setName("pod1");
        pod1.setAge(BigDecimal.ONE);
        PodWithAge pod2 = new PodWithAge(new ObjectMeta(), new PodSpec(), new PodStatus());
        pod2.getMetadata().setName("pod2");
        pod2.setAge(BigDecimal.ONE);
        PodWithAge pod3 = new PodWithAge(new ObjectMeta(), new PodSpec(), new PodStatus());
        pod3.getMetadata().setName("pod3");
        pod3.setAge(BigDecimal.TEN);
        PodNodeScheduler.podList = new PodListWithIndex(Arrays.asList(pod1, pod2, pod3));
        PodNodeScheduler.unscheduledPods = new PodListWithIndex(Arrays.asList(pod1, pod2, pod3));
        Node n1 = new Node();
        PodNodeScheduler.nodeList = new NodeList();
        KubernetesClient kubernetesClient = KubernetesClientSingleton.getKubernetesClient();//.bindings().create(b1)

        KubernetesClient clientMock = mock(KubernetesClient.class);
        //  when(clientMock.getKubernetesClient().bindings().create(b1)(anyString())).thenReturn(false);
        PodNodeScheduler.scheduleQueue(PodNodeScheduler.podList, PodNodeScheduler.nodeList);
        //   Assert.assertEquals(PodNodeScheduler.podList.getItems().size(), 2);
    }

    public void testGetStarvingPod() {
        PodWithAge pod1 = new PodWithAge(new ObjectMeta(), new PodSpec(), new PodStatus());
        pod1.getMetadata().setName("pod1");
        pod1.setAge(BigDecimal.ONE);
        PodWithAge pod2 = new PodWithAge(new ObjectMeta(), new PodSpec(), new PodStatus());
        pod2.getMetadata().setName("pod2");
        pod2.setAge(BigDecimal.ONE);
        PodWithAge pod3 = new PodWithAge(new ObjectMeta(), new PodSpec(), new PodStatus());
        pod3.getMetadata().setName("pod3");
        pod3.setAge(BigDecimal.TEN);
        PodNodeScheduler.podList = new PodListWithIndex(Arrays.asList(pod1, pod2, pod3));
        PodNodeScheduler.unscheduledPods = new PodListWithIndex(Arrays.asList(pod1, pod2, pod3));
        Node n1 = new Node();
        PodNodeScheduler.nodeList = new NodeList();

        Optional<Pod> starvingPod = PodNodeScheduler.getStarvingPod();
        Assert.assertTrue(starvingPod.isPresent());
        // pod3 is starving
        Assert.assertEquals(starvingPod.get().getMetadata().getName(), "pod3");
        Assert.assertEquals(((PodWithAge) starvingPod.get()).getAge(), BigDecimal.TEN);
        // pod3 is not on top of the queue
        Assert.assertEquals(PodNodeScheduler.podList.getItems().indexOf(pod3), 2);
    }

    public void testNoStarvingPodsDespiteHighAge() {
        PodWithAge pod1 = new PodWithAge(new ObjectMeta(), new PodSpec(), new PodStatus());
        pod1.getMetadata().setName("pod1");
        pod1.setAge(BigDecimal.TEN);
        PodWithAge pod2 = new PodWithAge(new ObjectMeta(), new PodSpec(), new PodStatus());
        pod2.getMetadata().setName("pod2");
        pod2.setAge(BigDecimal.TEN);
        PodWithAge pod3 = new PodWithAge(new ObjectMeta(), new PodSpec(), new PodStatus());
        pod3.getMetadata().setName("pod3");
        pod3.setAge(BigDecimal.TEN);
        PodNodeScheduler.podList = new PodListWithIndex(Arrays.asList(pod1, pod2, pod3));
        PodNodeScheduler.unscheduledPods = new PodListWithIndex(Arrays.asList(pod1, pod2, pod3));
        Node n1 = new Node();
        PodNodeScheduler.nodeList = new NodeList();

        Optional<Pod> starvingPod = PodNodeScheduler.getStarvingPod();
        Assert.assertTrue(starvingPod.isEmpty());
    }

}