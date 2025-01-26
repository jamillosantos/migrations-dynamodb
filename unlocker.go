package migrations_dynamodb

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/jamillosantos/migrations/v2"
)

type UnlockDynamoDBClient interface {
	DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
}

type unlocker struct {
	client                UnlockDynamoDBClient
	lockTableName, lockID string
}

func (u *unlocker) Unlock(ctx context.Context) error {
	_, err := u.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: &u.lockTableName,
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{
				Value: u.lockID,
			},
		},
	})
	var conditionalCheckFailedException *types.ConditionalCheckFailedException
	switch {
	case errors.As(err, &conditionalCheckFailedException):
		return migrations.ErrMigrationAlreadyExists
	case err != nil:
		return fmt.Errorf("failed to add migration: %w", err)
	}
	return err
}
