package kinesisx

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

type ListShardsAPIClient interface {
	ListShards(context.Context, *kinesis.ListShardsInput, ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error)
}

var _ ListShardsAPIClient = (*kinesis.Client)(nil)

type ListShardsPaginatorOptions struct {
	Limit int32
}

// ListShardsPaginator is a paginator for ListShards
type ListShardsPaginator struct {
	options   ListShardsPaginatorOptions
	client    ListShardsAPIClient
	params    *kinesis.ListShardsInput
	nextToken *string
	firstPage bool
}

// NewListShardsPaginator returns a new ListShardsPaginator
func NewListShardsPaginator(client ListShardsAPIClient, params *kinesis.ListShardsInput, optFns ...func(*ListShardsPaginatorOptions)) *ListShardsPaginator {
	if params == nil {
		params = &kinesis.ListShardsInput{}
	}

	options := ListShardsPaginatorOptions{}
	if params.MaxResults != nil {
		options.Limit = *params.MaxResults
	}

	for _, fn := range optFns {
		fn(&options)
	}

	return &ListShardsPaginator{
		options:   options,
		client:    client,
		params:    params,
		firstPage: true,
		nextToken: params.NextToken,
	}
}

// HasMorePages returns a boolean indicating whether more pages are available
func (p *ListShardsPaginator) HasMorePages() bool {
	return p.firstPage || (p.nextToken != nil && len(*p.nextToken) != 0)
}

// NextPage retrieves the next ListShards page.
func (p *ListShardsPaginator) NextPage(ctx context.Context, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error) {
	if !p.HasMorePages() {
		return nil, fmt.Errorf("no more pages available")
	}

	params := *p.params
	params.NextToken = p.nextToken

	var limit *int32
	if p.options.Limit > 0 {
		limit = &p.options.Limit
	}
	params.MaxResults = limit

	result, err := p.client.ListShards(ctx, &params, optFns...)
	if err != nil {
		return nil, err
	}
	p.firstPage = false
	p.nextToken = result.NextToken
	return result, nil
}
