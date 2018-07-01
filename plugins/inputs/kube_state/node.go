package kube_state

import (
	"context"
	"strconv"

	"github.com/influxdata/telegraf"
	"k8s.io/api/core/v1"
)

var (
	nodeMeasurement = "kube_node"
	nodeTaintMeasurement = "kube_node_spec_taint"
)

func registerNodeCollector(ctx context.Context, acc telegraf.Accumulator, ks *KubenetesState) {
	list, err := ks.client.getNodes(ctx)
	if err != nil {
		acc.AddError(err)
		return
	}
	for _, n := range list.Items {
		if err = ks.gatherNode(n, acc); err != nil {
			acc.AddError(err)
			return
		}
	}

}
func (ks *KubenetesState) gatherNode(n v1.Node, acc telegraf.Accumulator) error {
	fields := map[string]interface{}{}
	tags := map[string]string{
		"node":                      n.Name,
		"kernel_version":            n.Status.NodeInfo.KernelVersion,
		"os_image":                  n.Status.NodeInfo.OSImage,
		"container_runtime_version": n.Status.NodeInfo.ContainerRuntimeVersion,
		"kubelet_version":           n.Status.NodeInfo.KubeletVersion,
		"kubeproxy_version":         n.Status.NodeInfo.KubeProxyVersion,
		"provider_id":               n.Spec.ProviderID,
		"spec_unschedulable": strconv.FormatBool(n.Spec.Unschedulable)
	}

	if !n.CreationTimestamp.IsZero() {
		fields["created"] = n.CreationTimestamp.Unix()
	}

	for k, v := range n.Labels {
		tags["label_"+sanitizeLabelName(k)] = v
	}

	// Collect node taints
	for _, taint := range n.Spec.Taints {
		go gatherNodeTaint(n, taint, acc)
	}

	acc.AddFields(nodeMeasurement, fields, tags)
	return nil
}

func gatherNodeTaint(n v1.Node, taint v1.Taint,acc telegraf.Accumulator){
	fields := map[string]interface{}{
		"gauge":1,
	}
	tags := map[string]string{
		"node": n.Name,
		"key": taint.Key,
		"value": taint.Value,
		"effect":string(taint.Effect),
	}
	
	acc.AddFields(nodeTaintMeasurement, fields, tags)
	
}