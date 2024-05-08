package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/aws/smithy-go"
	mp "github.com/mackerelio/go-mackerel-plugin"
	"github.com/mashiike/mackerel-plugin-aws-kinesis-data-stream-shard-count/internal/kinesisx"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

type Plugin struct {
	Prefix     string
	StreamName string
	Client     *kinesis.Client
	Context    context.Context
}

func (p Plugin) GraphDefinition() map[string]mp.Graphs {
	labelPrefix := cases.Title(language.Und, cases.NoLower).String(p.MetricKeyPrefix())
	return map[string]mp.Graphs{
		"shards": {
			Label: labelPrefix + " Shards",
			Unit:  mp.UnitFloat,
			Metrics: []mp.Metrics{
				{Name: "count", Label: "Count"},
			},
		},
	}
}

func (p Plugin) FetchMetrics() (map[string]float64, error) {
	pagenator := kinesisx.NewListShardsPaginator(p.Client, &kinesis.ListShardsInput{
		StreamName: aws.String(p.StreamName),
		MaxResults: aws.Int32(100),
		ShardFilter: &types.ShardFilter{
			Type: types.ShardFilterTypeAtLatest,
		},
	})
	var count float64
	for pagenator.HasMorePages() {
		page, err := pagenator.NextPage(p.Context)
		if err != nil {
			var apiError smithy.APIError
			if errors.As(err, &apiError) {
				if apiError.ErrorCode() == "ResourceNotFoundException" {
					return map[string]float64{}, nil
				}
			}
			return nil, fmt.Errorf("failed to describe stream: %w", err)
		}
		count += float64(len(page.Shards))
	}
	return map[string]float64{"count": count}, nil
}

func (p Plugin) MetricKeyPrefix() string {
	if p.Prefix == "" {
		p.Prefix = "kinesis"
	}
	return p.Prefix
}

var Version string = "current"

func main() {
	if err := _main(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func _main() error {
	optPrefix := flag.String("metric-key-prefix", "", "Metric key prefix")
	optTempfile := flag.String("tempfile", "", "Temp file name")
	optStreamName := flag.String("stream-name", "", "Kinesis Stream name")
	optRegion := flag.String("region", "", "AWS Region")
	optVersion := flag.Bool("version", false, "Show version")
	flag.Parse()

	if *optVersion {
		fmt.Println("mackerel-plugin-aws-kinesis-data-stream-shard-count version:", Version)
		fmt.Println("go version:", runtime.Version())
		return nil
	}

	if *optStreamName == "" {
		return errors.New("stream-name is required")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	loadConfigOpts := []func(*config.LoadOptions) error{}
	if *optRegion != "" {
		loadConfigOpts = append(loadConfigOpts, config.WithRegion(*optRegion))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, loadConfigOpts...)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	u := Plugin{
		Prefix:     *optPrefix,
		StreamName: *optStreamName,
		Client:     kinesis.NewFromConfig(awsCfg),
		Context:    ctx,
	}
	plugin := mp.NewMackerelPlugin(u)
	plugin.Tempfile = *optTempfile
	plugin.Run()
	return nil
}
