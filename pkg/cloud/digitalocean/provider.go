package digitalocean

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"sync"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/clustercache"
	"github.com/opencost/opencost/pkg/env"
	v1 "k8s.io/api/core/v1"
)

const (
	BytesPerGB = 1024 * 1024 * 1024
	MBPerGB    = 1024
)

type DOProvider struct {
	Clientset               clustercache.ClusterCache
	Config                  models.ProviderConfig
	APIToken                string
	Pricing                 map[string]*DOPricing
	ClusterRegion           string
	ClusterAccountID        string
	DownloadPricingDataLock sync.RWMutex
}

type DOPricing struct {
	CPU          int     `json:"cpu"`
	RAM          float64 `json:"ram"`
	Price        float64 `json:"price"`
	Region       string  `json:"region"`
	ResourceType string  `json:"resource_type"`
}

type DOPricingResponse struct {
	Options struct {
		Sizes []struct {
			Slug         string   `json:"slug"`
			VCPUs        int      `json:"vcpus"`
			Memory       float64  `json:"memory"`
			PriceMonthly float64  `json:"price_monthly"`
			Regions      []string `json:"regions"`
		} `json:"sizes"`
	} `json:"options"`
}

func (do *DOProvider) GetConfig() (*models.CustomPricing, error) {
	return do.Config.GetCustomPricingData()
}

func (do *DOProvider) DownloadPricingData() error {
	do.DownloadPricingDataLock.Lock()
	defer do.DownloadPricingDataLock.Unlock()

	if do.Pricing == nil {
		do.Pricing = make(map[string]*DOPricing)
	}

	err := do.getPricingData()
	if err != nil {
		return err
	}

	clusterID := env.GetClusterID()
	if clusterID != "" {
		err = do.getClusterPricing(clusterID)
		if err != nil {
			log.Warnf("Failed to get cluster-specific pricing: %v", err)
		}
	}

	return nil
}

func (do *DOProvider) getPricingData() error {
	pricingURL := "https://api.digitalocean.com/v2/kubernetes/options"

	client := &http.Client{}
	req, err := http.NewRequest("GET", pricingURL, nil)
	if err != nil {
		return err
	}

	req.Header.Add("Authorization", "Bearer "+do.APIToken)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var pricing DOPricingResponse
	if err := json.NewDecoder(resp.Body).Decode(&pricing); err != nil {
		return err
	}

	for _, size := range pricing.Options.Sizes {
		do.Pricing[size.Slug] = &DOPricing{
			CPU:   size.VCPUs,
			RAM:   size.Memory,
			Price: size.PriceMonthly / timeutil.HoursPerMonth,
		}
	}

	return nil
}

func (do *DOProvider) getClusterPricing(clusterID string) error {
	clusterURL := fmt.Sprintf("https://api.digitalocean.com/v2/kubernetes/clusters/%s", clusterID)

	client := &http.Client{}
	req, err := http.NewRequest("GET", clusterURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	req.Header.Add("Authorization", "Bearer "+do.APIToken)

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to get cluster details: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to get cluster details. Status: %d, Body: %s", resp.StatusCode, string(body))
	}

	var cluster struct {
		Kubernetes struct {
			Region    string `json:"region"`
			NodePools []struct {
				Size  string `json:"size"`
				Count int    `json:"count"`
			} `json:"node_pools"`
			ClusterSubnet string   `json:"cluster_subnet"`
			ServiceSubnet string   `json:"service_subnet"`
			Tags          []string `json:"tags"`
		} `json:"kubernetes"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&cluster); err != nil {
		return fmt.Errorf("failed to decode cluster response: %v", err)
	}

	do.ClusterRegion = cluster.Kubernetes.Region

	for _, nodePool := range cluster.Kubernetes.NodePools {
		if pricing, exists := do.Pricing[nodePool.Size]; exists {
			pricing.Region = cluster.Kubernetes.Region
			pricing.ResourceType = "node"
		}
	}

	return nil
}

func (do *DOProvider) ClusterInfo() (map[string]string, error) {
	conf, err := do.GetConfig()
	if err != nil {
		return nil, err
	}

	m := make(map[string]string)
	m["provider"] = opencost.DigitalOceanProvider
	m["region"] = do.ClusterRegion
	m["account"] = do.ClusterAccountID
	if conf.ClusterName != "" {
		m["name"] = conf.ClusterName
	}
	m["id"] = env.GetClusterID()

	return m, nil
}

func (do *DOProvider) NodePricing(key models.Key) (*models.Node, models.PricingMetadata, error) {
	if len(do.Pricing) == 0 {
		if err := do.DownloadPricingData(); err != nil {
			return nil, models.PricingMetadata{}, fmt.Errorf("no pricing data available: %v", err)
		}
	}

	doKey, ok := key.(*digitalOceanKey)
	if !ok {
		return nil, models.PricingMetadata{}, fmt.Errorf("invalid key type %T", key)
	}

	instanceType, _ := util.GetInstanceType(doKey.Labels)
	if instanceType == "" {
		return nil, models.PricingMetadata{}, fmt.Errorf("could not get instance type from labels")
	}

	pricing, ok := do.Pricing[instanceType]
	if !ok {
		return nil, models.PricingMetadata{}, fmt.Errorf("no pricing data found for instance type: %s", instanceType)
	}

	node := &models.Node{
		Cost:         fmt.Sprintf("%f", pricing.Price),
		VCPU:         fmt.Sprintf("%f", float64(pricing.CPU)),
		RAM:          fmt.Sprintf("%f", pricing.RAM*MBPerGB),
		GPU:          "0",
		Storage:      "0",
		BaseCPUPrice: fmt.Sprintf("%f", pricing.Price/float64(pricing.CPU)),
		BaseRAMPrice: fmt.Sprintf("%f", pricing.Price/pricing.RAM),
		BaseGPUPrice: "0",
		UsageType:    "ondemand",
	}

	metadata := models.PricingMetadata{
		Source: string(opencost.DigitalOceanProvider),
	}

	return node, metadata, nil
}

const (
	regionalLBMonthlyPrice = 12.0
	globalLBMonthlyPrice   = 15.0

	// Global LB included limits (before overage charges apply)
	includedDataTransferGB   = 1000     // First 1000 GB included
	includedRequestsPerMonth = 25000000 // First 25M requests included
	includedDomainsPerMonth  = 5        // First 5 domains included

	// Global LB additional costs
	dataTransferOverageCostPerGB = 0.02
	requestOverageCostPerMillion = 0.70
	domainOverageCostPerDomain   = 0.14
)

type loadBalancerMetrics struct {
	Type         string  `json:"type"`
	DataTransfer float64 `json:"data_transfer_gb"`
	RequestCount int64   `json:"request_count"`
	DomainCount  int     `json:"domain_count"`
}

func (do *DOProvider) LoadBalancerPricing() (*models.LoadBalancer, error) {
	lbType, dataTransferGB, requestCount, domainCount, ipAddresses, err := do.getLBMetrics()
	if err != nil {
		return nil, err
	}

	var baseCost float64
	if lbType == "global" {
		baseCost = globalLBMonthlyPrice

		// Calculate overages for global LB
		if dataTransferGB > includedDataTransferGB {
			baseCost += (dataTransferGB - includedDataTransferGB) * dataTransferOverageCostPerGB
		}

		if requestCount > includedRequestsPerMonth {
			extraMillions := float64(requestCount-includedRequestsPerMonth) / 1000000
			baseCost += math.Ceil(extraMillions) * requestOverageCostPerMillion
		}

		if domainCount > includedDomainsPerMonth {
			baseCost += float64(domainCount-includedDomainsPerMonth) * domainOverageCostPerDomain
		}
	} else {
		// Default to regional LB
		baseCost = regionalLBMonthlyPrice
	}

	// Convert monthly cost to hourly
	hourlyCost := baseCost / timeutil.HoursPerMonth

	return &models.LoadBalancer{
		Cost:               hourlyCost,
		IngressIPAddresses: ipAddresses,
	}, nil
}

func (do *DOProvider) getLBMetrics() (string, float64, int64, int, []string, error) {
	services := do.Clientset.GetAllServices()

	var totalDataTransfer float64
	var totalRequests int64
	var totalDomains int
	var ipAddresses []string
	lbType := "regional"

	for _, svc := range services {
		if svc.Spec.Type != "LoadBalancer" {
			continue
		}

		// Get IP addresses from service status
		for _, ingress := range svc.Status.LoadBalancer.Ingress {
			address := ingress.IP
			if address == "" {
				address = ingress.Hostname
			}
			if address != "" {
				ipAddresses = append(ipAddresses, address)
			}
		}

		if t, ok := svc.Annotations["kubernetes.digitalocean.com/load-balancer-type"]; ok {
			if t == "global" {
				lbType = "global"
			}
		}

		lbID := ""
		if id, ok := svc.Annotations["kubernetes.digitalocean.com/load-balancer-id"]; ok {
			lbID = id
		}
		if lbID == "" {
			continue
		}

		metrics, err := do.fetchLoadBalancerMetrics(lbID)
		if err != nil {
			log.Warnf("Failed to fetch metrics for LB %s: %v", lbID, err)
			continue
		}

		totalDataTransfer += metrics.DataTransfer
		totalRequests += metrics.RequestCount
		if lbType == "global" {
			totalDomains += metrics.DomainCount
		}
	}

	return lbType, totalDataTransfer, totalRequests, totalDomains, ipAddresses, nil
}

func (do *DOProvider) fetchLoadBalancerMetrics(lbID string) (*loadBalancerMetrics, error) {
	// Base URL for Digital Ocean API
	baseURL := "https://api.digitalocean.com/v2"

	now := time.Now().UTC()
	hourAgo := now.Add(-1 * time.Hour)

	metricsURL := fmt.Sprintf("%s/load_balancers/%s/metrics?start_time=%s&end_time=%s",
		baseURL,
		lbID,
		hourAgo.Format(time.RFC3339),
		now.Format(time.RFC3339))

	client := &http.Client{}
	req, err := http.NewRequest("GET", metricsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}

	req.Header.Add("Authorization", "Bearer "+do.APIToken)
	req.Header.Add("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch metrics: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to get metrics. Status: %d, Body: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var metricsResponse struct {
		Metrics struct {
			DataTransferBytes float64  `json:"data_transfer_bytes"`
			RequestCount      int64    `json:"request_count"`
			Domains           []string `json:"domains"`
		} `json:"metrics"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&metricsResponse); err != nil {
		return nil, fmt.Errorf("failed to decode metrics response: %v", err)
	}

	// Convert bytes to GB
	dataTransferGB := metricsResponse.Metrics.DataTransferBytes / (BytesPerGB)

	return &loadBalancerMetrics{
		DataTransfer: dataTransferGB,
		RequestCount: metricsResponse.Metrics.RequestCount,
		DomainCount:  len(metricsResponse.Metrics.Domains),
	}, nil
}

type digitalOceanKey struct {
	Labels     map[string]string
	ProviderID string
}

func (k *digitalOceanKey) Features() string {
	instanceType, _ := util.GetInstanceType(k.Labels)
	region, _ := util.GetRegion(k.Labels)
	return region + "," + instanceType
}

func (k *digitalOceanKey) GPUType() string {
	return "" // DigitalOcean doesn't support GPUs
}

func (k *digitalOceanKey) GPUCount() int {
	return 0 // DigitalOcean doesn't support GPUs
}

func (k *digitalOceanKey) ID() string {
	return k.ProviderID
}

func (do *DOProvider) GetKey(labels map[string]string, n *v1.Node) models.Key {
	return &digitalOceanKey{
		Labels:     labels,
		ProviderID: n.Spec.ProviderID,
	}
}
