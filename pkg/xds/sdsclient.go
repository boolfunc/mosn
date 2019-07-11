package xds

import (
	"sync"

	"github.com/envoyproxy/go-control-plane/envoy/api/v2/core"

	"sofastack.io/sofa-mosn/pkg/types"

	"sofastack.io/sofa-mosn/pkg/log"

	"github.com/envoyproxy/go-control-plane/envoy/api/v2/auth"
	v2 "sofastack.io/sofa-mosn/pkg/xds/v2"
)

type SdsClientImpl struct {
	SdsConfigMap   map[string]*auth.SdsSecretConfig
	SdsCallbackMap map[string]v2.SdsUpdateCallbackFunc
	updatedLock    sync.Mutex
	sdsSubscriber  *v2.SdsSubscriber
}

var sdsClient *SdsClientImpl
var sdsClientLock sync.Mutex

// GetSdsClientImpl use by tls module , when get sds config from xds
func GetSdsClient(config auth.SdsSecretConfig) v2.SdsClient {
	if sdsClient != nil {
		return sdsClient
	} else {
		sdsClientLock.Lock()
		defer sdsClientLock.Unlock()
		sdsClient = &SdsClientImpl{
			SdsConfigMap:   make(map[string]*auth.SdsSecretConfig),
			SdsCallbackMap: make(map[string]v2.SdsUpdateCallbackFunc),
		}
		// For Istio , sds config should be the same
		// So we use first sds config to init sds subscriber
		sdsClient.sdsSubscriber = v2.NewSdsSubscriber(sdsClient, config.SdsConfig, ServiceNode, ServiceCluster)
		sdsClient.sdsSubscriber.Start()
	}
	return nil
}

// CloseSdsClientImpl used only mosn exit
func CloseSdsClient() {
	if sdsClient != nil && sdsClient.sdsSubscriber != nil {
		sdsClient.sdsSubscriber.Stop()
	}
}

func (client *SdsClientImpl) AddUpdateCallback(sdsConfig *auth.SdsSecretConfig, callback v2.SdsUpdateCallbackFunc) {
	client.updatedLock.Lock()
	defer client.updatedLock.Unlock()
	client.SdsConfigMap[sdsConfig.Name] = sdsConfig
	client.SdsCallbackMap[sdsConfig.Name] = callback
	client.sdsSubscriber.SendSdsRequest(sdsConfig.Name)
}

func (client *SdsClientImpl) DeleteUpdateCallback(sdsConfig *auth.SdsSecretConfig) {
	client.updatedLock.Lock()
	defer client.updatedLock.Unlock()
	delete(client.SdsConfigMap, sdsConfig.Name)
	delete(client.SdsCallbackMap, sdsConfig.Name)
}

// SetSecret invoked when sds subscriber get secret response
func (client *SdsClientImpl) SetSecret(name string, secret *auth.Secret) {
	if fc, ok := client.SdsCallbackMap[name]; ok {
		log.DefaultLogger.Debugf("[xds] [sds client],set secret = %v", name)
		mosnSecret := &types.SDSSecret{
			Name: secret.Name,
		}
		if validateSecret, ok := secret.Type.(*auth.Secret_ValidationContext); ok {
			ds := validateSecret.ValidationContext.TrustedCa.Specifier.(*core.DataSource_InlineBytes)
			mosnSecret.ValidationPEM = string(ds.InlineBytes)
		}
		if tlsCert, ok := secret.Type.(*auth.Secret_TlsCertificate); ok {
			certSpec, _ := tlsCert.TlsCertificate.CertificateChain.Specifier.(*core.DataSource_InlineBytes)
			priKey, _ := tlsCert.TlsCertificate.PrivateKey.Specifier.(*core.DataSource_InlineBytes)
			mosnSecret.CertificatePEM = string(certSpec.InlineBytes)
			mosnSecret.PrivateKeyPEM = string(priKey.InlineBytes)
		}
		fc(name, mosnSecret)
	}
}
