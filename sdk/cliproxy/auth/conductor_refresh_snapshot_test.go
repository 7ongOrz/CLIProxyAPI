package auth

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestManagerRefreshFailurePreservesReplacedCredential(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "unauthorized", err: &Error{HTTPStatus: http.StatusUnauthorized, Message: "refresh token expired"}},
		{name: "transient", err: &Error{HTTPStatus: http.StatusServiceUnavailable, Message: "upstream unavailable"}},
		{name: "canceled", err: context.Canceled},
	} {
		for _, replace := range []string{"update", "register", "pending_update"} {
			t.Run(tc.name+"/"+replace, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					ctx := context.Background()
					manager := NewManager(nil, &RoundRobinSelector{}, nil)
					started := make(chan struct{})
					release := make(chan struct{})
					releaseRefresh := sync.OnceFunc(func() { close(release) })
					defer releaseRefresh()
					manager.RegisterExecutor(&unauthorizedRefreshExecutor{
						id:         "codex",
						refreshErr: tc.err,
						onRefresh: func() {
							close(started)
							<-release
						},
					})
					auth := &Auth{
						ID:       "refresh-snapshot",
						Provider: "codex",
						Status:   StatusActive,
						Metadata: map[string]any{
							"access_token":             "old-access-token",
							"refresh_token":            "old-refresh-token",
							"expired":                  time.Now().Add(-time.Hour).Format(time.RFC3339),
							"refresh_interval_seconds": 300,
						},
					}
					if _, errRegister := manager.Register(ctx, auth); errRegister != nil {
						t.Fatalf("register credential: %v", errRegister)
					}
					refreshDone := make(chan error, 1)
					go func() {
						_, errRefresh := manager.refreshAuthForRequest(ctx, auth.ID, "")
						refreshDone <- errRefresh
					}()
					<-started

					current, ok := manager.GetByID(auth.ID)
					if !ok {
						t.Fatal("expected registered credential")
					}
					if _, scheduled := nextRefreshCheckAt(time.Now(), current, time.Second); !scheduled {
						t.Fatal("expected initial credential to be eligible for refresh")
					}
					// The replacement refresh token can renew its expired access token.
					current.Metadata["access_token"] = "new-access-token"
					current.Metadata["refresh_token"] = "new-refresh-token"
					var errReplace error
					switch replace {
					case "register":
						_, errReplace = manager.Register(ctx, current)
					case "pending_update":
						store := &blockingEnrichingAuthStore{entered: make(chan struct{}), release: make(chan struct{})}
						releaseSave := sync.OnceFunc(func() { close(store.release) })
						defer releaseSave()
						manager.SetStore(store)
						updateDone := make(chan error, 1)
						go func() {
							_, errUpdate := manager.Update(ctx, current)
							updateDone <- errUpdate
						}()
						<-store.entered
						releaseRefresh()
						synctest.Wait()
						releaseSave()
						errReplace = <-updateDone
					default:
						_, errReplace = manager.Update(ctx, current)
					}
					if errReplace != nil {
						t.Fatalf("replace credential: %v", errReplace)
					}

					releaseRefresh()
					if errRefresh := <-refreshDone; !errors.Is(errRefresh, tc.err) {
						t.Fatalf("refresh error = %v, want %v", errRefresh, tc.err)
					}
					updated, ok := manager.GetByID(auth.ID)
					if !ok {
						t.Fatal("expected replacement credential")
					}
					if updated.LastError != nil || updated.Unavailable || updated.Status != StatusActive {
						t.Fatalf("replacement state = status %s, unavailable %t, error %v; want active credential", updated.Status, updated.Unavailable, updated.LastError)
					}
					if !updated.NextRefreshAfter.IsZero() {
						t.Fatalf("replacement refresh delayed until %s, want immediately eligible", updated.NextRefreshAfter)
					}
					if _, scheduled := nextRefreshCheckAt(time.Now(), updated, time.Second); !scheduled {
						t.Fatal("expected replacement credential to remain scheduled for refresh")
					}
				})
			})
		}
	}
}
