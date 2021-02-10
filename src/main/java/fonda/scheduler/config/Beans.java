package fonda.scheduler.config;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class Beans {

    private static KubernetesClient kubernetesClient;

    @Bean
    KubernetesClient getClient(){
        if(kubernetesClient == null) {
            kubernetesClient = new DefaultKubernetesClient();
        }
        return kubernetesClient;
    }

}
