package openai

import "testing"

func TestResponsesLocalInterruptResponseOwnership(t *testing.T) {
	interrupt := newResponsesLocalInterrupt()
	if interrupt.handle("r1") {
		t.Fatal("accepted an interrupt before response.created")
	}
	interrupt.begin("r1")
	if interrupt.handle("old-response") {
		t.Fatal("accepted an interrupt for a different response")
	}
	if !interrupt.handle("r1") {
		t.Fatal("expected the active response to accept its interrupt")
	}
	if !interrupt.handle("r1") {
		t.Fatal("expected a duplicate interrupt to succeed")
	}
	if got := <-interrupt.interruptsChan(); got != "r1" {
		t.Fatalf("interrupted response = %q, want r1", got)
	}
	interrupt.end()
	interrupt.begin("r2")
	if !interrupt.handle("r1") {
		t.Fatal("expected a late interrupt for the previous response to succeed")
	}
	select {
	case responseID := <-interrupt.interruptsChan():
		t.Fatalf("late interrupt queued cancellation for %q", responseID)
	default:
	}
	if !interrupt.handle("r2") {
		t.Fatal("expected the current response to remain interruptible")
	}
	if got := <-interrupt.interruptsChan(); got != "r2" {
		t.Fatalf("interrupted response = %q, want r2", got)
	}
	interrupt.end()
}

func TestResponsesLocalInterruptAfterCompletion(t *testing.T) {
	interrupt := newResponsesLocalInterrupt()
	interrupt.begin("r1")
	interrupt.end()
	for i := 0; i < 2; i++ {
		if !interrupt.handle("r1") {
			t.Fatal("expected a completed response to accept a late interrupt")
		}
	}
	if interrupt.handle("unknown") {
		t.Fatal("accepted an interrupt for an unknown response")
	}
	interrupt.begin("r2")
	if !interrupt.handle("r1") {
		t.Fatal("expected a completed response to accept an interrupt during the next turn")
	}
	select {
	case responseID := <-interrupt.interruptsChan():
		t.Fatalf("completed response queued cancellation for %q", responseID)
	default:
	}
	if !interrupt.handle("r2") {
		t.Fatal("expected the active response to accept its interrupt")
	}
	if got := <-interrupt.interruptsChan(); got != "r2" {
		t.Fatalf("interrupted response = %q, want r2", got)
	}
	interrupt.end()
}

func TestResponsesLocalInterruptCompletionClearsPendingCancellation(t *testing.T) {
	interrupt := newResponsesLocalInterrupt()
	interrupt.begin("r1")
	if !interrupt.handle("r1") {
		t.Fatal("expected the active response to accept its interrupt")
	}
	// Completion can win the forwarder's select after the reader queues a cancel.
	interrupt.end()
	interrupt.begin("r2")
	select {
	case responseID := <-interrupt.interruptsChan():
		t.Fatalf("cancellation for %q leaked into the next response", responseID)
	default:
	}
	if !interrupt.handle("r2") {
		t.Fatal("expected the next response to accept its interrupt")
	}
	if got := <-interrupt.interruptsChan(); got != "r2" {
		t.Fatalf("interrupted response = %q, want r2", got)
	}
	interrupt.end()
}
