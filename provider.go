package awsssmprovider

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"go.opentelemetry.io/collector/confmap"
)

func New() confmap.Provider {
	return &provider{}
}

const (
	scheme    = "awsssm"
	delimiter = ","
)

var (
	ErrUnsupportedScheme = errors.New("unsupported scheme")
	yes                  = true
)

type provider struct {
	client *ssm.Client
}

var _ confmap.Provider = (*provider)(nil)

func (provider) Scheme() string {
	return scheme
}

func (p *provider) Retrieve(ctx context.Context, uri string, watcher confmap.WatcherFunc) (*confmap.Retrieved, error) {
	parsed, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("url.Parse: %w", err)
	}
	if parsed.Scheme != scheme {
		return nil, ErrUnsupportedScheme
	}

	if p.client == nil {
		initCtx, cancel := context.WithCancel(context.Background())
		if deadline, ok := ctx.Deadline(); ok {
			// copy original deadline but do not propagate cancellation from original context
			initCtx, cancel = context.WithDeadline(initCtx, deadline)
		}
		defer cancel()
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf("config.LoadDefaultConfig: %w", err)
		}
		p.client = ssm.NewFromConfig(cfg)
	}

	input := &ssm.GetParameterInput{Name: &parsed.Path, WithDecryption: &yes}
	out, err := p.client.GetParameter(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("ssm.Client.GetParameter: %w", err)
	}
	switch out.Parameter.Type {
	case types.ParameterTypeStringList:
		vals := strings.Split(*out.Parameter.Value, delimiter)
		return confmap.NewRetrieved(vals)
	default:
		return confmap.NewRetrieved(*out.Parameter.Value)
	}
}

func (p *provider) Shutdown(ctx context.Context) error {
	return nil
}
