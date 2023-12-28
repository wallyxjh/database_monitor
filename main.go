package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	feishuWebhookURL = "https://open.feishu.cn/open-apis/bot/v2/hook/39260980-fbea-4c1a-9f72-f75c372c1b73"
)

var (
	clientset     *kubernetes.Clientset
	dynamicClient *dynamic.DynamicClient
	// 记录上一次的数据库状态
	lastStatus = make(map[string]string)
	// 记录欠费的ns
	debtRecord = make(map[string]bool)
)

type FeishuMessage struct {
	MsgType string `json:"msg_type"`
	Content struct {
		Text string `json:"text"`
	} `json:"content"`
}

func main() {
	initClient()
	database_monitor()
}

func initClient() {
	// 使用 kubeconfig 连接 Kubernetes 集群
	config, err := clientcmd.BuildConfigFromFlags("", "/Users/james/go/src/github.com/wally/database-monitor/config/kubeconfig")
	if err != nil {
		panic(err.Error())
	}

	dynamicClient, err = dynamic.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	clientset, err = kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
}

func database_monitor() {

	// CRD GVR
	gvr := schema.GroupVersionResource{
		Group:    "apps.kubeblocks.io",
		Version:  "v1alpha1",
		Resource: "clusters",
	}

	for {
		// 每隔 5 分钟执行一次
		checkDatabases(gvr)
		time.Sleep(5 * time.Minute)
	}
}

func checkDatabases(gvr schema.GroupVersionResource) {
	// clusters, err := dynamicClient.Resource(gvr).Namespace(namespace).List(context.Background(), metav1.ListOptions{})
	clusters, err := dynamicClient.Resource(gvr).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		panic(err.Error())
	}

	database_message := fmt.Sprintf("%-50s %-50s %-50s\n", "DatabaseName", "Status", "Namespace")
	for _, cluster := range clusters.Items {
		status, found, err := unstructured.NestedString(cluster.Object, "status", "phase")
		name, namespace := cluster.GetName(), cluster.GetNamespace()
		if err != nil || !found {
			fmt.Printf("Unable to get %s status in ns %s: %v\n", name, namespace, err)
			continue
		}
		if status == "Running" || status == "Stopped" {
			delete(lastStatus, name)
			continue
		}
		if _, ok := lastStatus[name]; !ok {
			// 如果 lastStatus 中不存在 name，直接更新状态
			lastStatus[name] = status
			continue
		}
		if status == "Failed" && !debtRecord[namespace] {
			_, debt := checkQuota(namespace)
			if !debt {
				database_message += fmt.Sprintf("%-50s %-50s %-50s\n", name, status, namespace)
				continue
			}

			debtRecord[namespace] = true
			delete(lastStatus, name)
			continue
		}
		database_message += fmt.Sprintf("%-50s %-50s %-50s\n", name, status, namespace)
		// 更新状态
		lastStatus[name] = status
	}
	// 如果数据库依然处于异常状态，则发送通知
	err = sendFeishuNotification(database_message)
	if err != nil {
		fmt.Printf("Error sending notification: %v\n", err)
	} else {
		fmt.Println("Notification sent successfully")
	}
}

func checkQuota(ns string) (error, bool) {
	resourceQuotasClient := clientset.CoreV1().ResourceQuotas(ns)

	// 查找名为 "debt-limit0" 的 ResourceQuota
	resourceQuota, err := resourceQuotasClient.Get(context.TODO(), "debt-limit0", metav1.GetOptions{})
	if err != nil {
		// 处理错误：资源不存在或其他错误。
		fmt.Printf("Error getting ResourceQuota: %s\n", err.Error())
		return err, false
	}
	return nil, resourceQuota != nil
}

func sendFeishuNotification(database_message string) error {

	message := FeishuMessage{
		MsgType: "text",
		Content: struct {
			Text string `json:"text"`
		}{
			Text: database_message,
		},
	}
	// 序列化消息为 JSON
	messageBytes, err := json.Marshal(message)
	if err != nil {
		fmt.Printf("Error marshalling message: %v\n", err)
		return err
	}

	// 发送 POST 请求到 Feishu Webhook
	resp, err := http.Post(feishuWebhookURL, "application/json", bytes.NewBuffer(messageBytes))
	if err != nil {
		fmt.Printf("Error sending alert to Feishu: %v\n", err)
		return err
	}
	defer resp.Body.Close()

	// 检查响应状态
	if resp.StatusCode != http.StatusOK {
		fmt.Printf("Failed to send alert. Status code: %d\n", resp.StatusCode)
	} else {
		fmt.Println("Alert sent successfully")
	}
	return nil
}
