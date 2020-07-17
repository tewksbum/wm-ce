package dsrcluster

import (
	"strings"
	"github.com/golang/protobuf/ptypes/duration"
	"encoding/json"
	"net/http"
	"context"
	"log"
	"os"
	"fmt"
	"strconv"
	"io/ioutil"

	dataproc "cloud.google.com/go/dataproc/apiv1"
	"google.golang.org/api/option"
	"cloud.google.com/go/pubsub"
	"github.com/ybbus/httpretry"
	dataprocpb "google.golang.org/genproto/googleapis/cloud/dataproc/v1"
)

func parseInt(input string, def int) int {
	val, err := strconv.Atoi(input)
	if err != nil {
		return def
	}
	return val
}

func parseCommaDelimitedNameValuePair(input string) map[string]string {
	list := strings.Split(input, ",") 
	result := make(map[string]string)
	for _, item := range list {
		nvp := strings.Split(item, "=")
		result[nvp[0]] = nvp[1]
	}
	return result
}

func Run(ctx context.Context, m *pubsub.Message) error {
	region := os.Getenv("REGION")

	// Create the cluster client.
	endpoint := region + "-dataproc.googleapis.com:443"
	clusterClient, err := dataproc.NewClusterControllerClient(ctx, option.WithEndpoint(endpoint))
	if err != nil {
		log.Fatalf("dataproc.NewClusterControllerClient: %v", err)
	}

	// Create the cluster config.
	req := &dataprocpb.CreateClusterRequest{
		ProjectId: os.Getenv("GCP_PROJECT"),
		Region:    region,
		Cluster: &dataprocpb.Cluster{
				ProjectId:   os.Getenv("GCP_PROJECT"),
				ClusterName: os.Getenv("CLUSTER_NAME"),
				Config: &dataprocpb.ClusterConfig{
					ConfigBucket: os.Getenv("BUCKET"),
					GceClusterConfig: &dataprocpb.GceClusterConfig {
						ServiceAccount: os.Getenv("SERVICE_ACCOUNT"),
						ServiceAccountScopes:  strings.Split(os.Getenv("SCOPES"), ","),
						// ZoneUri: os.Getenv("ZONE"),
						SubnetworkUri: "default",
					},
					MasterConfig: &dataprocpb.InstanceGroupConfig{
							NumInstances:   int32(parseInt(os.Getenv("MASTER_COUNT"), 2)),
							MachineTypeUri: os.Getenv("MASTER_MACHINE_TYPE"),
							MinCpuPlatform: os.Getenv("MASTER_CPU_PLATFORM"),
							DiskConfig: &dataprocpb.DiskConfig{
								BootDiskType: os.Getenv("MASTER_BOOT_DISK_TYPE"),
								BootDiskSizeGb: int32(parseInt(os.Getenv("MASTER_BOOT_DISK_SIZE"), 100)),
								NumLocalSsds: int32(parseInt(os.Getenv("MASTER_LOCAL_SSD_COUNT"), 2)),
							},
					},
					WorkerConfig: &dataprocpb.InstanceGroupConfig{
							NumInstances:   int32(parseInt(os.Getenv("WORKER_COUNT"), 2)),
							MachineTypeUri: os.Getenv("WORKER_MACHINE_TYPE"),
							MinCpuPlatform: os.Getenv("WORKER_CPU_PLATFORM"),
							DiskConfig: &dataprocpb.DiskConfig{
								BootDiskType: os.Getenv("WORKER_BOOT_DISK_TYPE"),
								BootDiskSizeGb: int32(parseInt(os.Getenv("WORKER_BOOT_DISK_SIZE"), 100)),
								NumLocalSsds: int32(parseInt(os.Getenv("WORKER_LOCAL_SSD_COUNT"), 2)),
							},								
					},
					LifecycleConfig: &dataprocpb.LifecycleConfig{	// delete after 20 min of inactivity
						IdleDeleteTtl: &duration.Duration {
							Seconds: int64(parseInt(os.Getenv("IDLE_DELETE_MINUTE"), 20) * 60),
						},
					},
					SoftwareConfig: &dataprocpb.SoftwareConfig {
						ImageVersion: os.Getenv("IMAGE_VERSION"),
						Properties: parseCommaDelimitedNameValuePair(os.Getenv("SPARK_PROPERTIES")),
					},
				},
		},
	}

	// Create the cluster.
	op, err := clusterClient.CreateCluster(ctx, req)
	reqJSON, _ := json.Marshal(req)
	log.Printf("request %v", string(reqJSON))
	if err != nil {
		log.Fatalf("CreateCluster: %v", err)
	}

	resp, err := op.Wait(ctx)
	if err != nil {
		log.Fatalf("CreateCluster.Wait: %v", err)
	}

	// Output a success message.
	log.Printf("Cluster created successfully: %s", resp.ClusterName)

	// submit the job
	jobJSON := `
	{
		"projectId": "wemade-core",
		"job": {
			"placement": {
			"clusterName": "sparkles"
			},
			"statusHistory": [],
			"reference": {
			"jobId": "ocm-ns-orders",
			"projectId": "wemade-core"
			},
			"sparkJob": {
				"mainClass": "streamer.OrderStreamer",
				"properties": {
					"spark.jars.packages": "org.apache.spark:spark-sql_2.12:3.0.0"
				},
				"jarFileUris": [
					"gs://wm_dataproc/netsuite-orders-1.0-SNAPSHOT.jar"
				],
				"args": [
					"runonce"
				],
			}
		}
	}
	`
	jobURL := fmt.Sprintf("https://%v/v1/projects/wemade-core/regions/us-central1/jobs:submit/", endpoint)
	jobReq, _ := http.NewRequest("POST", jobURL, strings.NewReader(jobJSON))
	jobReq.Header.Set("Content-Type", "application/json")
	jobReq.Header.Set("Accept", "application/json")
	client := httpretry.NewDefaultClient()
	jobResp, err := client.Do(jobReq)
	if err != nil {
		log.Fatalf("FATAL ERROR Unable to send request to netsuite: error %v", err)
	}
	defer jobResp.Body.Close()
	jobBody, err := ioutil.ReadAll(jobResp.Body)
	log.Println(string(jobBody))

	return nil	
}

