package saga_golang_data_storage_aws_dynamodb

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/al-kimmel-serj/saga-golang"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type DataStorage[Data any] struct {
	dynamodbClient *dynamodb.Client
	tableName      string
}

func NewDataStorage[Data any](dynamodbClient *dynamodb.Client, tableName string) *DataStorage[Data] {
	return &DataStorage[Data]{
		dynamodbClient: dynamodbClient,
		tableName:      tableName,
	}
}

func (s *DataStorage[Data]) Load(ctx context.Context, sagaID saga.ID) (*Data, error) {
	item, err := s.dynamodbClient.GetItem(ctx, &dynamodb.GetItemInput{
		Key:       s.buildKey(sagaID),
		TableName: &s.tableName,
	})
	if err != nil {
		return nil, fmt.Errorf("dynamodb client get item error: %w", err)
	}

	dataAttr := item.Item["data"].(*types.AttributeValueMemberB)
	data := new(Data)
	err = json.Unmarshal(dataAttr.Value, data)
	if err != nil {
		return nil, fmt.Errorf("json unmarshal saga data attribute value error: %w", err)
	}

	return data, nil
}

func (s *DataStorage[Data]) Save(ctx context.Context, sagaID saga.ID, data *Data) error {
	dataInJSON, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("json marshal saga data attribute value error: %w", err)
	}

	_, err = s.dynamodbClient.PutItem(ctx, &dynamodb.PutItemInput{
		Item: map[string]types.AttributeValue{
			"saga_id": &types.AttributeValueMemberS{Value: string(sagaID)},
			"data":    &types.AttributeValueMemberB{Value: dataInJSON},
		},
		TableName: &s.tableName,
	})
	if err != nil {
		return fmt.Errorf("dynamodb client put item error: %w", err)
	}

	return nil
}

func (s *DataStorage[Data]) buildKey(sagaID saga.ID) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"saga_id": &types.AttributeValueMemberS{Value: string(sagaID)},
	}
}
