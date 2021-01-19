package tuberlin.dos.kubscheduler;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;

import java.util.*;

public class PodListWithIndex extends PodList {

    private Map<String, Pod> nameIndexMap = new HashMap<>(); // Welche Map ist hier am effizientesten

    public PodListWithIndex () {

    }

    public void addPodToList(Pod pod) {
        nameIndexMap.put(pod.getMetadata().getName(), pod);
    }

    public void removePodFromList(Pod pod) {
        nameIndexMap.remove(pod.getMetadata().getName());
    }


    @Override
    public List<Pod> getItems() {
        return new ArrayList<>(nameIndexMap.values());
    }
}