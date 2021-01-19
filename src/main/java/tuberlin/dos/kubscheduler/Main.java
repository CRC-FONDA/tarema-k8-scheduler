package tuberlin.dos.kubscheduler;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;

public class Main {

    public static void main(String[] args) {

        KubernetesClient client = new DefaultKubernetesClient();

        CurrentPodNodeStatus nodeInformation = new CurrentPodNodeStatus(client);

    }
}
