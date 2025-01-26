package migrations_dynamodb

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	transport "github.com/aws/smithy-go/endpoints"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "migrations/dynamodb")
}

var (
	dynamoDBClient *dynamodb.Client
)

var _ = BeforeSuite(func() {
	ctx := context.Background()

	awsConfig, err := config.LoadDefaultConfig(ctx,
		config.WithDefaultRegion("sa-region-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("abcdef", "`12345", ""),
		),
	)
	Expect(err).NotTo(HaveOccurred())

	dynamoDBClient = dynamodb.NewFromConfig(awsConfig, dynamodb.WithEndpointResolverV2(endpointResolver("http://localhost:8000")))
})

type endpointResolver string

func (e endpointResolver) ResolveEndpoint(_ context.Context, _ dynamodb.EndpointParameters) (transport.Endpoint, error) {
	u, err := url.Parse(string(e))
	if err != nil {
		return transport.Endpoint{}, fmt.Errorf("could not parse the endpoint URL for DynamoDB: %w", err)
	}
	return transport.Endpoint{
		URI: *u,
	}, nil
}
