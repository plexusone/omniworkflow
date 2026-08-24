package temporal_test

import (
	"context"
	"os"
	"testing"

	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"

	"github.com/plexusone/omniworkflow/conformance"
	"github.com/plexusone/omniworkflow/temporal"
)

// TestTemporalConformance runs the shared conformance suite against a real
// Temporal client+worker+server. It needs a Temporal backend, provided one of
// two ways:
//
//   - OMNIWORKFLOW_TEMPORAL_DEVSERVER=1 starts an ephemeral dev server (needs
//     the Temporal CLI, downloaded on first use).
//   - OMNIWORKFLOW_TEMPORAL_HOSTPORT=host:port connects to an existing server.
//
// With neither set it skips, keeping `go test ./...` hermetic. The
// Temporal-specific workflow logic is covered hermetically by the
// TestInterpreterWorkflow* tests in the test environment.
func TestTemporalConformance(t *testing.T) {
	client := temporalClientForTest(t)
	defer client.Close()

	provider := temporal.New(client, "omniworkflow-conftest")
	conformance.Run(t, conformance.Config{
		Provider:  provider,
		NewWorker: provider.NewWorker,
	})
}

func temporalClientForTest(t *testing.T) sdkclient.Client {
	t.Helper()

	if os.Getenv("OMNIWORKFLOW_TEMPORAL_DEVSERVER") != "" {
		server, err := testsuite.StartDevServer(context.Background(), testsuite.DevServerOptions{})
		if err != nil {
			t.Fatalf("start dev server: %v", err)
		}
		t.Cleanup(func() { _ = server.Stop() })
		return server.Client()
	}

	if hostPort := os.Getenv("OMNIWORKFLOW_TEMPORAL_HOSTPORT"); hostPort != "" {
		client, err := sdkclient.Dial(sdkclient.Options{HostPort: hostPort})
		if err != nil {
			t.Fatalf("dial Temporal at %s: %v", hostPort, err)
		}
		return client
	}

	t.Skip("set OMNIWORKFLOW_TEMPORAL_DEVSERVER=1 or OMNIWORKFLOW_TEMPORAL_HOSTPORT=host:port to run the live Temporal conformance suite")
	return nil
}
