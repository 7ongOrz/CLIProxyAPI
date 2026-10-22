package openai

import "sync"

// responsesLocalInterrupt binds HTTP cancellation to the response.created ID.
// Known IDs remain valid for late interrupts throughout the downstream session.
type responsesLocalInterrupt struct {
	mu               sync.Mutex
	responseID       string
	knownResponseIDs map[string]struct{}
	interrupts       chan string
}

func newResponsesLocalInterrupt() *responsesLocalInterrupt {
	return &responsesLocalInterrupt{
		knownResponseIDs: make(map[string]struct{}),
		interrupts:       make(chan string, 1),
	}
}

func (s *responsesLocalInterrupt) begin(responseID string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.responseID = responseID
	if responseID != "" {
		s.knownResponseIDs[responseID] = struct{}{}
	}
	s.mu.Unlock()
}

func (s *responsesLocalInterrupt) end() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.responseID = ""
	select {
	case <-s.interrupts:
	default:
	}
	s.mu.Unlock()
}

func (s *responsesLocalInterrupt) handle(responseID string) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, known := s.knownResponseIDs[responseID]; !known {
		return false
	}
	if s.responseID == responseID {
		// Each response queues one cancellation. end drains it before the next turn.
		s.responseID = ""
		s.interrupts <- responseID
	}
	return true
}

func (s *responsesLocalInterrupt) interruptsChan() <-chan string {
	if s == nil {
		return nil
	}
	return s.interrupts
}
