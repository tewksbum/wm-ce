package dsrcluster

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/golang/protobuf/ptypes/duration"

	dataproc "cloud.google.com/go/dataproc/apiv1"
	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
	"google.golang.org/api/option"
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
				GceClusterConfig: &dataprocpb.GceClusterConfig{
					ServiceAccount:       os.Getenv("SERVICE_ACCOUNT"),
					ServiceAccountScopes: strings.Split(os.Getenv("SCOPES"), ","),
					// ZoneUri: os.Getenv("ZONE"),
					SubnetworkUri: "default",
				},
				MasterConfig: &dataprocpb.InstanceGroupConfig{
					NumInstances:   int32(parseInt(os.Getenv("MASTER_COUNT"), 2)),
					MachineTypeUri: os.Getenv("MASTER_MACHINE_TYPE"),
					MinCpuPlatform: os.Getenv("MASTER_CPU_PLATFORM"),
					DiskConfig: &dataprocpb.DiskConfig{
						BootDiskType:   os.Getenv("MASTER_BOOT_DISK_TYPE"),
						BootDiskSizeGb: int32(parseInt(os.Getenv("MASTER_BOOT_DISK_SIZE"), 100)),
						NumLocalSsds:   int32(parseInt(os.Getenv("MASTER_LOCAL_SSD_COUNT"), 2)),
					},
				},
				WorkerConfig: &dataprocpb.InstanceGroupConfig{
					NumInstances:   int32(parseInt(os.Getenv("WORKER_COUNT"), 2)),
					MachineTypeUri: os.Getenv("WORKER_MACHINE_TYPE"),
					MinCpuPlatform: os.Getenv("WORKER_CPU_PLATFORM"),
					DiskConfig: &dataprocpb.DiskConfig{
						BootDiskType:   os.Getenv("WORKER_BOOT_DISK_TYPE"),
						BootDiskSizeGb: int32(parseInt(os.Getenv("WORKER_BOOT_DISK_SIZE"), 100)),
						NumLocalSsds:   int32(parseInt(os.Getenv("WORKER_LOCAL_SSD_COUNT"), 2)),
					},
				},
				LifecycleConfig: &dataprocpb.LifecycleConfig{ // delete after 20 min of inactivity
					IdleDeleteTtl: &duration.Duration{
						Seconds: int64(parseInt(os.Getenv("IDLE_DELETE_MINUTE"), 20) * 60),
					},
				},
				SoftwareConfig: &dataprocpb.SoftwareConfig{
					ImageVersion: os.Getenv("IMAGE_VERSION"),
					Properties:   parseCommaDelimitedNameValuePair(os.Getenv("SPARK_PROPERTIES")),
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

	// Create the job client.
	jobClient, err := dataproc.NewJobControllerClient(ctx, option.WithEndpoint(endpoint))

	// Create the job config.
	submitJobReq := &dataprocpb.SubmitJobRequest{
		ProjectId: os.Getenv("GCP_PROJECT"),
		Region:    region,
		Job: &dataprocpb.Job{
			Reference: &dataprocpb.JobReference{
				JobId: "ocm-ns-orders-" + uuid.New().String(),
			},
			Placement: &dataprocpb.JobPlacement{
				ClusterName: os.Getenv("CLUSTER_NAME"),
			},
			TypeJob: &dataprocpb.Job_SparkJob{
				SparkJob: &dataprocpb.SparkJob{
					JarFileUris: []string{
						os.Getenv("JOB_JARFILE"),
					},
					Driver: &dataprocpb.SparkJob_MainClass{
						MainClass: os.Getenv("JOB_CLASS"),
					},
					Args: []string{
						os.Getenv("JOB_ARGUMENT"),
					},
					Properties: map[string]string{
						"spark.jars.packages": "org.apache.spark:spark-sql_2.12:3.0.0",
					},
				},
			},
		},
	}

	submitJobResp, err := jobClient.SubmitJob(ctx, submitJobReq)
	if err != nil {
		log.Printf("error submitting job: %v", err)
		return nil
	}

	id := submitJobResp.Reference.JobId

	log.Printf("Submitted job %q", id)

	return nil
}
