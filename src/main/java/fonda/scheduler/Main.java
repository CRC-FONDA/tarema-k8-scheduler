package fonda.scheduler;

import fonda.scheduler.distributedscheduler.TaremaScheduler;
import io.fabric8.kubernetes.client.KubernetesClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;

@SpringBootApplication
@Slf4j
public class Main {

    public static void main(String[] args) {

        SpringApplication.run(Main.class, args);

    }

    private final KubernetesClient client;

    public Main( @Autowired KubernetesClient client ) {
        this.client = client;
    }

    @PostConstruct
    public void afterStart(){
        final TaremaScheduler podNodeScheduler = new TaremaScheduler("new-scheduler", client);
    }
}
