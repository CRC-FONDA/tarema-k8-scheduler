package fonda.scheduler.controller;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;

public class KubernetesClientSingleton {

    private static KubernetesClient kubernetesClient;

    private KubernetesClientSingleton() {

    }

    public static KubernetesClient getKubernetesClient() {
        if(kubernetesClient == null) {
            kubernetesClient = new DefaultKubernetesClient();
        }
        return kubernetesClient;
    }


}
