package auth

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/auth/codex"
)

type codexPartitionRefreshExecutor struct {
	unauthorizedRefreshExecutor
	siwc               bool
	retainRefreshToken bool
	rotateAccessToken  bool
}

func (e *codexPartitionRefreshExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	updated := auth.Clone()
	partition := updated.Metadata
	if e.siwc {
		partition = updated.Metadata["siwc"].(map[string]any)
	}
	// A successful refresh can retain its access token while rotating its refresh token.
	if !e.retainRefreshToken {
		partition["refresh_token"] = "rotated-refresh-token"
	}
	if e.rotateAccessToken {
		partition["access_token"] = "rotated-access-token"
	}
	partition["expired"] = time.Now().Add(time.Hour).Format(time.RFC3339)
	updated.CodexRefreshPatch = codex.NewCredentialRefreshPatch(auth.Metadata, updated.Metadata, !e.siwc, e.siwc)
	return updated, nil
}

func TestManagerCodexPartialRefreshRecoversActivePartition(t *testing.T) {
	for _, tc := range []struct {
		name               string
		useSIWC            bool
		rejectedOuter      bool
		refreshedSIWC      bool
		retainRefreshToken bool
		rotateAccessToken  bool
		wantRecovered      bool
	}{
		{name: "active_outer_refreshed", wantRecovered: true},
		{name: "inactive_siwc_refreshed", refreshedSIWC: true},
		{name: "active_siwc_refreshed", useSIWC: true, refreshedSIWC: true, rotateAccessToken: true, wantRecovered: true},
		{name: "inactive_outer_refreshed", useSIWC: true},
		{name: "active_outer_tokens_retained", retainRefreshToken: true, wantRecovered: true},
		{name: "active_siwc_access_retained", useSIWC: true, refreshedSIWC: true},
		{name: "active_siwc_tokens_retained", useSIWC: true, refreshedSIWC: true, retainRefreshToken: true},
		{name: "codex_outer_refreshed_with_siwc_enabled", useSIWC: true, rejectedOuter: true, rotateAccessToken: true, wantRecovered: true},
		{name: "codex_outer_tokens_retained_with_siwc_enabled", useSIWC: true, rejectedOuter: true, retainRefreshToken: true, wantRecovered: true},
		{name: "siwc_refresh_preserves_codex_outer_failure", useSIWC: true, rejectedOuter: true, refreshedSIWC: true, rotateAccessToken: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			manager := NewManager(&simpleCodexDiskStore{path: filepath.Join(t.TempDir(), "codex.json")}, &RoundRobinSelector{}, nil)
			manager.RegisterExecutor(&codexPartitionRefreshExecutor{
				unauthorizedRefreshExecutor: unauthorizedRefreshExecutor{id: "codex"},
				siwc:                        tc.refreshedSIWC,
				retainRefreshToken:          tc.retainRefreshToken,
				rotateAccessToken:           tc.rotateAccessToken,
			})
			auth := &Auth{
				ID:            "partial-refresh",
				Provider:      "codex",
				Status:        StatusError,
				Unavailable:   true,
				StatusMessage: "unauthorized",
				LastError:     &Error{HTTPStatus: http.StatusUnauthorized, Message: "token rejected"},
				Metadata: map[string]any{
					"access_token":  "outer-access",
					"refresh_token": "outer-refresh",
					"use_siwc":      tc.useSIWC,
					"siwc": map[string]any{
						"access_token":  "siwc-access",
						"refresh_token": "siwc-refresh",
						"client_id":     "siwc-client",
					},
				},
			}
			auth.RejectedAccessToken = authAccessToken(auth)
			if tc.rejectedOuter {
				auth.RejectedAccessToken = authOuterAccessToken(auth)
			}
			if _, errRegister := manager.Register(ctx, auth); errRegister != nil {
				t.Fatal(errRegister)
			}
			refreshed, errRefresh := manager.ForceRefreshAuth(ctx, auth.ID)
			if errRefresh != nil {
				t.Fatal(errRefresh)
			}
			partition := refreshed.Metadata
			if tc.refreshedSIWC {
				partition = refreshed.Metadata["siwc"].(map[string]any)
			}
			wantRefreshToken := "rotated-refresh-token"
			if tc.retainRefreshToken {
				wantRefreshToken = "outer-refresh"
				if tc.refreshedSIWC {
					wantRefreshToken = "siwc-refresh"
				}
			}
			if partition["refresh_token"] != wantRefreshToken {
				t.Fatal("successful partition refresh must be retained")
			}
			if tc.wantRecovered {
				if refreshed.RejectedAccessToken != "" || refreshed.Unavailable || refreshed.Status != StatusActive || refreshed.LastError != nil {
					t.Fatalf("active partition refresh: status=%s unavailable=%t rejected=%t error=%v; want active credential",
						refreshed.Status, refreshed.Unavailable, refreshed.RejectedAccessToken != "", refreshed.LastError)
				}
			} else {
				if refreshed.RejectedAccessToken != auth.RejectedAccessToken || !refreshed.Unavailable || refreshed.Status != StatusError || refreshed.LastError == nil {
					t.Fatalf("preserved credential rejection: status=%s unavailable=%t rejected=%t error=%v; want original token rejection",
						refreshed.Status, refreshed.Unavailable, refreshed.RejectedAccessToken == auth.RejectedAccessToken, refreshed.LastError)
				}
			}
			if tc.useSIWC {
				wantRejectedSIWC := "siwc-access"
				if tc.wantRecovered || tc.rejectedOuter {
					wantRejectedSIWC = ""
				}
				if refreshed.RejectedSIWCAccessToken != wantRejectedSIWC {
					t.Fatal("SIWC rejection should follow the active access token")
				}
			}
		})
	}
}

func TestMergeCodexRefreshPreservesConcurrentActiveFailure(t *testing.T) {
	base := &Auth{
		Provider: "codex",
		Status:   StatusActive,
		Metadata: map[string]any{
			"access_token": "outer-old",
			"use_siwc":     true,
			"siwc":         map[string]any{"access_token": "siwc-old"},
		},
	}
	updated := base.Clone()
	updated.Metadata["access_token"] = "outer-refreshed"
	updated.Metadata["siwc"].(map[string]any)["access_token"] = "siwc-refreshed"
	updated.CodexRefreshPatch = codex.NewCredentialRefreshPatch(base.Metadata, updated.Metadata, true, true)

	current := base.Clone()
	current.Metadata["siwc"].(map[string]any)["access_token"] = "siwc-replacement"
	current.Status = StatusError
	current.Unavailable = true
	current.LastError = &Error{HTTPStatus: http.StatusUnauthorized, Message: "replacement token rejected"}
	current.RejectedAccessToken = "siwc-replacement"

	merged := MergeRefreshedAuth(base, current, updated)
	if merged.Metadata["access_token"] != "outer-refreshed" || authAccessToken(merged) != "siwc-replacement" {
		t.Fatal("merge should retain both the outer refresh and the concurrent SIWC replacement")
	}
	if merged.RejectedAccessToken != current.RejectedAccessToken || !merged.Unavailable || merged.Status != StatusError || merged.LastError == nil {
		t.Fatalf("stale SIWC refresh: status=%s unavailable=%t rejected=%t error=%v; want current failure",
			merged.Status, merged.Unavailable, merged.RejectedAccessToken == current.RejectedAccessToken, merged.LastError)
	}
}

func TestManagerCodexReloadRecoversActiveCredential(t *testing.T) {
	for _, mode := range []string{"outer", "siwc"} {
		for _, change := range []string{"metadata", "inactive_refresh", "active_refresh", "switch_active"} {
			t.Run(mode+"/"+change, func(t *testing.T) {
				ctx := WithSkipPersist(context.Background())
				manager := NewManager(nil, &RoundRobinSelector{}, nil)
				useSIWC := mode == "siwc"
				expires := time.Now().Add(time.Hour).Format(time.RFC3339)
				auth := &Auth{
					ID:                  "reload-partition",
					Provider:            "codex",
					Status:              StatusError,
					Unavailable:         true,
					StatusMessage:       "unauthorized",
					LastError:           &Error{HTTPStatus: http.StatusUnauthorized, Message: "token rejected"},
					NextRetryAfter:      time.Now().Add(time.Hour),
					cooldownRecordedSeq: 42,
					Metadata: map[string]any{
						"access_token":  "outer-access",
						"refresh_token": "outer-refresh",
						"expired":       expires,
						"use_siwc":      useSIWC,
						"siwc": map[string]any{
							"access_token":  "siwc-access",
							"refresh_token": "siwc-refresh",
							"client_id":     "siwc-client",
							"expired":       expires,
						},
					},
				}
				registered, errRegister := manager.Register(ctx, auth)
				if errRegister != nil {
					t.Fatal(errRegister)
				}
				metadata := cloneMetadataMap(registered.Metadata)
				switch change {
				case "metadata":
					metadata["note"] = "updated note"
				case "switch_active":
					metadata["use_siwc"] = !useSIWC
				default:
					partition := metadata
					refreshSIWC := useSIWC
					if change == "inactive_refresh" {
						refreshSIWC = !useSIWC
					}
					if refreshSIWC {
						partition = metadata["siwc"].(map[string]any)
					}
					partition["refresh_token"] = "rotated-refresh-token"
				}
				data, errMarshal := json.Marshal(metadata)
				if errMarshal != nil {
					t.Fatal(errMarshal)
				}
				path := filepath.Join(t.TempDir(), "codex.json")
				if errWrite := os.WriteFile(path, data, 0o600); errWrite != nil {
					t.Fatal(errWrite)
				}
				// The file watcher reconstructs enabled credentials with active status.
				snapshot := &Auth{
					ID: registered.ID, Provider: "codex", Status: StatusActive, Metadata: metadata,
				}
				snapshot.BindFileSnapshot(path, data)
				reloaded, errUpdate := manager.Update(ctx, snapshot)
				if errUpdate != nil {
					t.Fatal(errUpdate)
				}
				recovered := change == "active_refresh" || change == "switch_active"
				if recovered {
					if reloaded.Status != StatusActive || reloaded.Unavailable || reloaded.LastError != nil {
						t.Fatal("replacement active credential should restore availability")
					}
					if !reloaded.NextRetryAfter.IsZero() || reloaded.cooldownRecordedSeq != 0 {
						t.Fatal("replacement active credential should clear its authentication cooldown")
					}
				} else {
					if reloaded.Status != StatusError || !reloaded.Unavailable || reloaded.LastError == nil || reloaded.LastError.HTTPStatus != http.StatusUnauthorized {
						t.Fatal("reload with the same active credential should preserve its authentication failure")
					}
					if !reloaded.NextRetryAfter.Equal(registered.NextRetryAfter) || reloaded.cooldownRecordedSeq != registered.cooldownRecordedSeq {
						t.Fatal("reload with the same active credential should preserve its cooldown and watermark")
					}
				}
				if change == "inactive_refresh" && reloaded.CredentialVersion <= registered.CredentialVersion {
					t.Fatal("inactive credential changes should advance the persistence version")
				}
			})
		}
	}
}

func TestManagerCodexUpdateUsesPersistedCredentials(t *testing.T) {
	for _, mode := range []string{"outer", "siwc", "codex_outer_with_siwc_enabled"} {
		for _, change := range []string{"metadata", "active_credential", "inactive_credential"} {
			t.Run(mode+"/"+change, func(t *testing.T) {
				ctx := context.Background()
				path := filepath.Join(t.TempDir(), "codex.json")
				manager := NewManager(&simpleCodexDiskStore{path: path}, nil, nil)
				registered, errRegister := manager.Register(WithSkipPersist(ctx), &Auth{
					ID: "persisted-partition", Provider: "codex", Status: StatusError, Unavailable: true,
					LastError: &Error{HTTPStatus: http.StatusUnauthorized, Message: "token rejected"},
					Metadata: map[string]any{
						"access_token": "outer-access", "refresh_token": "outer-refresh",
						"use_siwc": mode != "outer",
						"siwc":     map[string]any{"access_token": "siwc-access", "refresh_token": "siwc-refresh", "client_id": "siwc-client"},
					},
				})
				if errRegister != nil {
					t.Fatal(errRegister)
				}
				rejectedToken := authOuterAccessToken(registered)
				if mode == "siwc" {
					rejectedToken = authSIWCTokenField(registered, "access_token")
				}
				manager.markRejectedAccessToken(registered.ID, rejectedToken)
				registered, _ = manager.GetByID(registered.ID)
				disk := registered.Clone()
				if change != "metadata" {
					rotateSIWC := mode == "siwc"
					if change == "inactive_credential" {
						rotateSIWC = !rotateSIWC
					}
					partition := disk.Metadata
					if rotateSIWC {
						partition = disk.Metadata["siwc"].(map[string]any)
					}
					partition["access_token"] = "disk-access"
					partition["refresh_token"] = "disk-refresh"
				}
				// A second process rotates credentials before the file watcher reloads them.
				if errWrite := codex.WriteJSONFileAtomic(path, disk.Metadata); errWrite != nil {
					t.Fatal(errWrite)
				}
				updated := registered.Clone()
				updated.Metadata["note"] = "updated note"
				saved, errUpdate := manager.Update(ctx, updated)
				if errUpdate != nil {
					t.Fatal(errUpdate)
				}
				published, _ := manager.GetByID(registered.ID)
				for name, result := range map[string]*Auth{"returned": saved, "published": published} {
					if CredentialsChanged(disk, result) || result.Metadata["note"] != "updated note" {
						t.Errorf("%s should contain the persisted credentials and metadata edit", name)
					}
					if change == "active_credential" {
						if result.Status != StatusActive || result.Unavailable || result.LastError != nil {
							t.Errorf("%s should recover after loading the replacement active credential", name)
						}
					} else if !hasUnauthorizedAuthFailure(result) {
						t.Errorf("%s should retain the unchanged active credential's failure", name)
					}
					wantVersion := registered.CredentialVersion
					if change != "metadata" {
						wantVersion++
					}
					if result.CredentialVersion != wantVersion {
						t.Errorf("%s credential version = %d, want %d", name, result.CredentialVersion, wantVersion)
					}
				}
			})
		}
	}
}
