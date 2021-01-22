package tuberlin.dos.kubscheduler;

import io.fabric8.kubernetes.api.model.Pod;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;

public class PodWithAge extends Pod {

    @Getter
    @Setter
    private BigDecimal age;


    PodWithAge(Pod pod) {
        super(pod.getApiVersion(), pod.getKind(), pod.getMetadata(), pod.getSpec(), pod.getStatus());
        this.age = BigDecimal.ZERO;
    }
}
