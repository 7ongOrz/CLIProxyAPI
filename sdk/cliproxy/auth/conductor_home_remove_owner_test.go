package auth

import (
	"context"
	"testing"
)

func TestRemoveIfCleansHomeRuntimeAuthOwners(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil, nil, nil)

	auth1, errReg1 := manager.Register(ctx, &Auth{
		ID:       "auth-1",
		Provider: "codex",
		Metadata: map[string]any{"access_token": "token-1"},
	})
	if errReg1 != nil {
		t.Fatalf("Register(auth-1) error = %v", errReg1)
	}
	auth2, errReg2 := manager.Register(ctx, &Auth{
		ID:       "auth-2",
		Provider: "codex",
		Metadata: map[string]any{"access_token": "token-2"},
	})
	if errReg2 != nil {
		t.Fatalf("Register(auth-2) error = %v", errReg2)
	}

	sel1 := &HomeDispatchSelection{Auth: auth1.Clone()}
	sel2 := &HomeDispatchSelection{Auth: auth2.Clone()}

	manager.rememberHomeSelectionRuntimeAuth("session-solo", sel1)
	manager.rememberHomeSelectionRuntimeAuth("session-shared", sel1)
	manager.rememberHomeSelectionRuntimeAuth("session-shared", sel2)

	if removed := manager.RemoveIf(ctx, "auth-1", nil); !removed {
		t.Fatal("RemoveIf(auth-1) = false, want true")
	}

	manager.mu.RLock()
	_, soloAuthExists := manager.homeRuntimeAuths["session-solo"]
	_, soloOwnerExists := manager.homeRuntimeAuthOwners["session-solo"]
	_, sharedAuth1Exists := manager.homeRuntimeAuths["session-shared"]["auth-1"]
	_, sharedOwner1Exists := manager.homeRuntimeAuthOwners["session-shared"]["auth-1"]
	sharedAuth2 := manager.homeRuntimeAuths["session-shared"]["auth-2"]
	sharedOwner2 := manager.homeRuntimeAuthOwners["session-shared"]["auth-2"]
	manager.mu.RUnlock()

	if soloAuthExists {
		t.Fatal("session-solo still present in homeRuntimeAuths after RemoveIf")
	}
	if soloOwnerExists {
		t.Fatal("session-solo still present in homeRuntimeAuthOwners after RemoveIf")
	}
	if sharedAuth1Exists {
		t.Fatal("auth-1 still present in homeRuntimeAuths[session-shared] after RemoveIf")
	}
	if sharedOwner1Exists {
		t.Fatal("auth-1 still present in homeRuntimeAuthOwners[session-shared] after RemoveIf")
	}
	if sharedAuth2 == nil || sharedAuth2.ID != "auth-2" {
		t.Fatalf("homeRuntimeAuths[session-shared][auth-2] = %#v, want auth-2", sharedAuth2)
	}
	if sharedOwner2 != sel2 {
		t.Fatalf("homeRuntimeAuthOwners[session-shared][auth-2] = %p, want %p", sharedOwner2, sel2)
	}

}

func TestHomeRuntimeAuthSnapshotFollowsSelectionLifecycle(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	selection := &HomeDispatchSelection{Auth: &Auth{ID: "auth-1", Provider: "codex"}}
	manager.rememberHomeSelectionRuntimeAuth("session-1", selection)
	if manager.homeRuntimeAuthOwners["session-1"]["auth-1"] != selection {
		t.Fatal("active snapshot must belong to its selection")
	}

	selection.End("upstream_disconnected")
	manager.forgetHomeRuntimeAuth("session-1", "auth-1", selection)
	// A retention attempt may reach publication after End has completed cleanup.
	manager.rememberHomeSelectionRuntimeAuth("session-1", selection)
	if len(manager.homeRuntimeAuths) != 0 || len(manager.homeRuntimeAuthOwners) != 0 {
		t.Fatal("ended selection republished its runtime credentials")
	}
}
