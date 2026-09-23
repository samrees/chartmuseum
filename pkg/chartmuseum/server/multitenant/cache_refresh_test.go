package multitenant

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chartmuseum/storage"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	helm_chart "helm.sh/helm/v3/pkg/chart"
	helm_repo "helm.sh/helm/v3/pkg/repo"

	cm_logger "helm.sh/chartmuseum/pkg/chartmuseum/logger"
	cm_router "helm.sh/chartmuseum/pkg/chartmuseum/router"
	cm_repo "helm.sh/chartmuseum/pkg/repo"
)

type refreshTestBackend struct {
	storage.Backend
	list func() ([]storage.Object, error)
	get  func(string) (storage.Object, error)
}

func (b *refreshTestBackend) ListObjects(string) ([]storage.Object, error)  { return b.list() }
func (b *refreshTestBackend) GetObject(path string) (storage.Object, error) { return b.get(path) }

func refreshTestServer(t *testing.T) (*MultiTenantServer, *cacheEntry, cm_logger.LoggingFn) {
	t.Helper()
	logger := &cm_logger.Logger{SugaredLogger: zap.NewNop().Sugar()}
	server := &MultiTenantServer{
		Router:             &cm_router.Router{},
		Logger:             logger,
		Tenants:            make(map[string]*tenantInternals),
		TenantCacheKeyLock: &sync.Mutex{},
	}
	log := logger.ContextLoggingFn(&gin.Context{})
	entry, err := server.initCacheEntry(log, "")
	require.NoError(t, err)
	return server, entry, log
}

func refreshTestChart(name string) *helm_repo.ChartVersion {
	return &helm_repo.ChartVersion{
		Metadata: &helm_chart.Metadata{Name: name, Version: "1.0.0"},
		URLs:     []string{"charts/" + name + "-1.0.0.tgz"},
		Created:  time.Unix(1000, 0),
		Digest:   "initial",
	}
}

func awaitRefreshTest(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("operation blocked")
	}
}

func TestRefreshPreservesConcurrentMutations(t *testing.T) {
	for _, scenario := range []string{"upload", "delete", "overwrite", "upload-then-delete", "delete-then-upload", "foreground-regeneration"} {
		t.Run(scenario, func(t *testing.T) {
			server, entry, log := refreshTestServer(t)
			chart := refreshTestChart("concurrent")
			stable := refreshTestChart("stable")
			removed := refreshTestChart("removed-externally")
			entry.RepoIndex.AddEntry(stable)
			entry.RepoIndex.AddEntry(removed)
			if scenario != "upload" && scenario != "upload-then-delete" {
				entry.RepoIndex.AddEntry(chart)
			}
			require.NoError(t, entry.RepoIndex.Regenerate())

			// An unrelated storage addition must be reconciled in this same pass,
			// even while API events are changing the index.
			var archive bytes.Buffer
			gz := gzip.NewWriter(&archive)
			tw := tar.NewWriter(gz)
			metadata := "apiVersion: v2\nname: added-externally\nversion: 1.0.0\n"
			require.NoError(t, tw.WriteHeader(&tar.Header{Name: "added-externally/Chart.yaml", Mode: 0600, Size: int64(len(metadata))}))
			_, err := tw.Write([]byte(metadata))
			require.NoError(t, err)
			require.NoError(t, tw.Close())
			require.NoError(t, gz.Close())
			added := storage.Object{Path: "added-externally-1.0.0.tgz", Content: archive.Bytes(), LastModified: chart.Created}
			listing := []storage.Object{cm_repo.StorageObjectFromChartVersion(stable), added}
			if scenario != "upload" && scenario != "delete-then-upload" {
				stale := cm_repo.StorageObjectFromChartVersion(chart)
				// Force an update candidate even with a timestamp tolerance; the
				// mutation journal must protect the event's digest and metadata.
				stale.LastModified = stale.LastModified.Add(time.Hour)
				listing = append(listing, stale)
			}
			started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			var listings atomic.Int32
			server.StorageBackend = &refreshTestBackend{
				list: func() ([]storage.Object, error) {
					listings.Add(1)
					close(started)
					<-release
					return listing, nil
				},
				get: func(path string) (storage.Object, error) {
					if path != added.Path {
						return storage.Object{}, errors.New("attempt to load a concurrently mutated chart")
					}
					return added, nil
				},
			}
			go func() { server.refreshCacheEntry(log, "", entry); close(done) }()
			awaitRefreshTest(t, started)

			// Reads and writes must complete before allowing the listing to finish.
			readDone := make(chan struct{})
			go func() { _, _ = server.getIndexFile(log, ""); close(readDone) }()
			awaitRefreshTest(t, readDone)
			mutated := make(chan struct{})
			mutationErr := make(chan error, 1)
			go func() {
				defer close(mutated)
				apply := func(op operationType) error {
					return server.applyCacheEvent(log, entry, event{OpType: op, ChartVersion: chart})
				}
				var err error
				switch scenario {
				case "upload":
					err = apply(addChart)
				case "delete":
					err = apply(deleteChart)
				case "overwrite":
					// Repeated writes must not force refresh retries or starvation.
					for i := 0; i < 100; i++ {
						copy := *chart
						copy.Digest = "uploaded"
						chart = &copy
						if err = apply(updateChart); err != nil {
							break
						}
					}
				case "upload-then-delete":
					if err = apply(addChart); err == nil {
						err = apply(deleteChart)
					}
				case "delete-then-upload":
					if err = apply(deleteChart); err == nil {
						err = apply(addChart)
					}
				case "foreground-regeneration":
					entry.RepoLock.Lock()
					result := <-server.regenerateRepositoryIndex(log, entry, storage.ObjectSliceDiff{
						Change: true, Removed: []storage.Object{cm_repo.StorageObjectFromChartVersion(chart)},
					})
					entry.RepoLock.Unlock()
					err = result.err
				}
				mutationErr <- err
			}()
			awaitRefreshTest(t, mutated)
			require.NoError(t, <-mutationErr)
			unblock()
			awaitRefreshTest(t, done)
			require.EqualValues(t, 1, listings.Load(), "refresh must finish without retrying")
			require.Nil(t, entry.refreshChanges)
			require.True(t, entry.RepoIndex.HasEntry(stable))
			require.False(t, entry.RepoIndex.HasEntry(removed))
			require.True(t, entry.RepoIndex.HasEntry(refreshTestChart("added-externally")))
			wantPresent := scenario == "upload" || scenario == "overwrite" || scenario == "delete-then-upload"
			require.Equal(t, wantPresent, entry.RepoIndex.HasEntry(chart))
			if scenario == "overwrite" {
				require.Equal(t, "uploaded", entry.RepoIndex.Entries[chart.Name][0].Digest)
			}
		})
	}
}

func TestRefreshNoChangesAndListingError(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "unchanged", true: "listing-error"}[fail], func(t *testing.T) {
			server, entry, log := refreshTestServer(t)
			chart := refreshTestChart("stable")
			entry.RepoIndex.AddEntry(chart)
			require.NoError(t, entry.RepoIndex.Regenerate())
			original, raw := entry.RepoIndex, string(entry.RepoIndex.Raw)
			server.UseStatefiles = true // No-change/error paths must not write a statefile.
			server.StorageBackend = &refreshTestBackend{list: func() ([]storage.Object, error) {
				if fail {
					return nil, errors.New("listing failed")
				}
				return []storage.Object{cm_repo.StorageObjectFromChartVersion(chart)}, nil
			}}
			done := make(chan struct{})
			go func() {
				server.refreshCacheEntry(log, "", entry)
				server.refreshCacheEntry(log, "", entry) // Locks/tracking must also be released on early returns.
				close(done)
			}()
			awaitRefreshTest(t, done)
			require.Same(t, original, entry.RepoIndex)
			require.Equal(t, raw, string(entry.RepoIndex.Raw))
			require.Nil(t, entry.refreshChanges)
			require.True(t, entry.RepoLock.TryLock())
			entry.RepoLock.Unlock()
		})
	}
}
