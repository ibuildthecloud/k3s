package rootlessports

import (
	"context"

	coreClients "github.com/rancher/wrangler/pkg/generated/controllers/core/v1"
)

func Register(ctx context.Context, serviceController coreClients.ServiceController, enabled bool, httpsPort int) error {
	panic("Rootless is not supported on windows")
}
