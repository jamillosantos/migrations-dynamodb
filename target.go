package migrations_dynamodb

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/jamillosantos/migrations/v2"
)

type DynamoDBClient interface {
	Scan(ctx context.Context, input *dynamodb.ScanInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
	PutItem(ctx context.Context, input *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	DeleteItem(ctx context.Context, input *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	UpdateItem(ctx context.Context, input *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)

	CreateTable(ctx context.Context, input *dynamodb.CreateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error)
	DeleteTable(ctx context.Context, input *dynamodb.DeleteTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteTableOutput, error)
	ListTables(ctx context.Context, d *dynamodb.ListTablesInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ListTablesOutput, error)
}

type Target struct {
	client DynamoDBClient

	tableName     string
	lockTableName string
	lockID        string
}

func NewTarget(client DynamoDBClient, opts ...Option) *Target {
	options := defaultOpts()
	for _, opt := range opts {
		opt(&options)
	}
	return &Target{
		client: client,

		tableName:     options.tableName,
		lockTableName: options.lockTableName,
		lockID:        options.lockID,
	}
}

// Current will return the current migration ID. If there is no current migration, it will return a
// migrations.ErrNoCurrentMigration error. Also, this implementation uses Done, so all errors Done would return
// can be returned by this method.
func (t *Target) Current(ctx context.Context) (string, error) {
	done, err := t.Done(ctx)
	if err != nil {
		return "", err
	}

	if len(done) == 0 {
		return "", migrations.ErrNoCurrentMigration
	}

	return done[len(done)-1], nil
}

// Create will create the migrations table and the migrations lock table in the DynamoDB.
func (t *Target) Create(ctx context.Context) error {
	listTableResponse, err := t.client.ListTables(ctx, &dynamodb.ListTablesInput{})
	if err != nil {
		return fmt.Errorf("failed to list tables: %w", err)
	}

	tables := make(map[string]struct{})
	for _, tableName := range listTableResponse.TableNames {
		tables[tableName] = struct{}{}
	}

	if _, ok := tables[t.tableName]; !ok {
		_, err := t.client.CreateTable(ctx, &dynamodb.CreateTableInput{
			TableName: &t.tableName,
			AttributeDefinitions: []types.AttributeDefinition{
				{
					AttributeName: aws.String("id"),
					AttributeType: types.ScalarAttributeTypeS,
				},
			},
			KeySchema: []types.KeySchemaElement{
				{
					AttributeName: aws.String("id"),
					KeyType:       types.KeyTypeHash,
				},
			},
			ProvisionedThroughput: &types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(1),
				WriteCapacityUnits: aws.Int64(1),
			},
		})
		if err != nil {
			return fmt.Errorf("failed to create migrations table: %w", err)
		}
	}

	if _, ok := tables[t.lockTableName]; !ok {
		_, err = t.client.CreateTable(ctx, &dynamodb.CreateTableInput{
			TableName: &t.lockTableName,
			AttributeDefinitions: []types.AttributeDefinition{
				{
					AttributeName: aws.String("id"),
					AttributeType: types.ScalarAttributeTypeS,
				},
			},
			KeySchema: []types.KeySchemaElement{
				{
					AttributeName: aws.String("id"),
					KeyType:       types.KeyTypeHash,
				},
			},
			ProvisionedThroughput: &types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(1),
				WriteCapacityUnits: aws.Int64(1),
			},
		})
		if err != nil {
			return fmt.Errorf("failed to create migrations lock table: %w", err)
		}
	}

	return nil
}

// Destroy will delete the migrations table and the migrations lock table in the DynamoDB.
func (t *Target) Destroy(ctx context.Context) error {
	_, err := t.client.DeleteTable(ctx, &dynamodb.DeleteTableInput{
		TableName: &t.tableName,
	})
	if err != nil {
		return fmt.Errorf("failed to delete migrations table: %w", err)
	}

	_, err = t.client.DeleteTable(ctx, &dynamodb.DeleteTableInput{
		TableName: &t.lockTableName,
	})
	if err != nil {
		return fmt.Errorf("failed to delete migrations lock table: %w", err)
	}

	return nil
}

// Done will list all migrations IDs done in the target. If a dirty migration is found, it will return an
// `migrations.ErrDirtyMigration`.
// The result will sorted by ID.
func (t *Target) Done(ctx context.Context) ([]string, error) {
	r := make([]string, 0)
	scanResponse, err := t.client.Scan(ctx, &dynamodb.ScanInput{
		TableName: &t.tableName,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to scan migrations table: %w", err)
	}

	var migration ddbMigration
	for _, item := range scanResponse.Items {
		err = attributevalue.UnmarshalMap(item, &migration)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal item: %w", err)
		}

		if migration.Dirty {
			return nil, migrations.ErrDirtyMigration
		}

		r = append(r, migration.ID)
	}

	sort.Sort(sort.StringSlice(r))
	return r, nil
}

func (t *Target) Add(ctx context.Context, id string) error {
	_, err := t.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &t.tableName,
		Item: map[string]types.AttributeValue{
			"id":    &types.AttributeValueMemberS{Value: id},
			"dirty": &types.AttributeValueMemberBOOL{Value: true},
		},
		ConditionExpression: aws.String("attribute_not_exists(id)"),
	})
	// if the record already exists, we can ignore the error.
	var conditionalCheckFailedException *types.ConditionalCheckFailedException
	switch {
	case errors.As(err, &conditionalCheckFailedException):
		return migrations.ErrMigrationAlreadyExists
	case err != nil:
		return fmt.Errorf("failed to add migration: %w", err)
	}

	return nil
}

// Remove will remove a migration from the target. If the migration does not exist, it returns an `migrations.ErrMigrationNotFound`.
func (t *Target) Remove(ctx context.Context, id string) error {
	_, err := t.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: &t.tableName,
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: id},
		},
		ConditionExpression: aws.String("attribute_exists(id)"),
	})
	var conditionalCheckFailedException *types.ConditionalCheckFailedException
	switch {
	case errors.As(err, &conditionalCheckFailedException):
		return migrations.ErrMigrationNotFound
	case err != nil:
		return fmt.Errorf("failed to remove migration: %w", err)
	}

	return nil
}

// FinishMigration will mark a migration as finished (dirty = false). If the migration does not exist, it will return an `migrations.ErrMigrationNotFound`.
func (t *Target) FinishMigration(ctx context.Context, id string) error {
	_, err := t.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: &t.tableName,
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: id},
		},
		UpdateExpression: aws.String("SET dirty = :dirty"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":dirty": &types.AttributeValueMemberBOOL{Value: false},
		},
		ConditionExpression: aws.String("attribute_exists(id)"),
	})
	var conditionalCheckFailedException *types.ConditionalCheckFailedException
	switch {
	case errors.As(err, &conditionalCheckFailedException):
		return migrations.ErrMigrationNotFound
	case err != nil:
		return fmt.Errorf("failed to finish migration: %w", err)
	}

	return nil
}

// StartMigration will mark a migration as started (dirty = true). If the migration does not exist, it will return an `migrations.ErrMigrationNotFound`.
func (t *Target) StartMigration(ctx context.Context, id string) error {
	_, err := t.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: &t.tableName,
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: id},
		},
		UpdateExpression: aws.String("SET dirty = :dirty"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":dirty": &types.AttributeValueMemberBOOL{Value: true},
		},
		ConditionExpression: aws.String("attribute_exists(id)"),
	})
	var conditionalCheckFailedException *types.ConditionalCheckFailedException
	switch {
	case errors.As(err, &conditionalCheckFailedException):
		return migrations.ErrMigrationNotFound
	case err != nil:
		return fmt.Errorf("failed to start migration: %w", err)
	}

	return nil
}

func (t *Target) Lock(ctx context.Context) (migrations.Unlocker, error) {
	for {
		_, err := t.client.PutItem(context.WithoutCancel(ctx), &dynamodb.PutItemInput{
			TableName: &t.lockTableName,
			Item: map[string]types.AttributeValue{
				"id": &types.AttributeValueMemberS{Value: t.lockID},
			},
			ConditionExpression: aws.String("attribute_not_exists(id)"),
		})
		var conditionalCheckFailedException *types.ConditionalCheckFailedException
		switch {
		case errors.As(err, &conditionalCheckFailedException):
			time.Sleep(time.Second)
			continue
		case err != nil:
			return nil, fmt.Errorf("failed to lock before migrating: %w", err)
		}
		break
	}

	return &unlocker{
		client:        t.client,
		lockTableName: t.lockTableName,
		lockID:        t.lockID,
	}, nil
}
