package tuberlin.dos.kubscheduler;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PodListWithIndex extends PodList {

    private ConcurrentMap<String, Pod> nameIndexMap = new ConcurrentHashMap<>(); // Welche Map ist hier am effizientesten

    public PodListWithIndex() {

    }

    public PodListWithIndex(List<Pod> pods) {
        pods.forEach(pod -> nameIndexMap.putIfAbsent(pod.getMetadata().getName(), pod));
    }

    /**
     * Overrides existing pod if pod is already in list
     *
     * @param pod insert pod into the concurrent map
     */
    public void addPodToList(Pod pod) {
        nameIndexMap.putIfAbsent(pod.getMetadata().getName(), pod);
    }

    public void removePodFromList(Pod pod) {
        nameIndexMap.remove(pod.getMetadata().getName());
    }


    @Override
    public List<Pod> getItems() {
        return new ArrayList<>(nameIndexMap.values());
    }
}
