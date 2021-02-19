package fonda.scheduler.model;

import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.Quantity;

import java.math.BigDecimal;

public class NodeWithAlloc implements Comparable<NodeWithAlloc> {

    private String nodeName;

    private BigDecimal current_cpu_usage;

    private BigDecimal current_ram_usage;

    private BigDecimal free_cpu;

    private BigDecimal free_ram;

    private Node node;

    public NodeWithAlloc(Node node) {
        this.node = node;
        this.nodeName = node.getMetadata().getName();
        this.current_ram_usage = new BigDecimal(0);
        this.current_cpu_usage = new BigDecimal(0);
        this.free_ram = new BigDecimal(0);
        this.free_cpu = new BigDecimal(0);

    }

    public BigDecimal getCurrent_cpu_usage() {
        return current_cpu_usage;
    }

    public void setCurrent_cpu_usage(BigDecimal current_cpu_usage) {
        this.current_cpu_usage = current_cpu_usage;
    }

    public BigDecimal getCurrent_ram_usage() {
        return current_ram_usage;
    }

    public void setCurrent_ram_usage(BigDecimal current_ram_usage) {
        this.current_ram_usage = current_ram_usage;
    }

    public Node getNode() {
        return node;
    }

    public void setNode(Node node) {
        this.node = node;
    }

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public BigDecimal getFree_cpu() {
        return free_cpu;
    }

    public Double getFree_cpu_as_Double() {
        return this.free_cpu.doubleValue();
    }

    public Double getCurrent_cpu_as_Double() {
        return this.current_cpu_usage.doubleValue();
    }

    public void setFree_cpu(BigDecimal free_cpu) {
        this.free_cpu = free_cpu;
    }

    public BigDecimal getFree_ram() {
        return free_ram;
    }

    public void setFree_ram(BigDecimal free_ram) {
        this.free_ram = free_ram;
    }

    public void calculateAlloc() {

        Quantity max_cpu = this.node.getStatus().getAllocatable().get("cpu");
        Quantity max_ram = this.node.getStatus().getAllocatable().get("memory");

        this.free_cpu = Quantity.getAmountInBytes(max_cpu).subtract(current_cpu_usage);
        this.free_ram = Quantity.getAmountInBytes(max_ram).subtract(current_ram_usage);

    }

    @Override
    public String toString() {
        return "NodeWithAlloc{" +
                "nodeName='" + nodeName + '\'' +
                ", current_cpu_usage=" + current_cpu_usage +
                ", current_ram_usage=" + current_ram_usage +
                ", free_cpu=" + free_cpu +
                ", free_ram=" + free_ram +
                ", node=" + node +
                '}';
    }

    @Override
    public int compareTo(NodeWithAlloc o) {
        if(getNode().getMetadata().getName().equals(o.getNode().getMetadata().getName())) {
            return 0;
        } else {
            return -1;
        }
    }
}
