package openai

import "sync"

const responsesLocalInterruptMaxKnownIDs = 64

// responsesLocalInterrupt binds HTTP cancellation to the response.created ID.
// Recent IDs remain valid for late interrupts throughout the downstream session.
type responsesLocalInterrupt struct {
	mu               sync.Mutex
	responseID       string
	knownResponseIDs map[string]struct{}
	knownOrder       []string
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
	select {
	case <-s.interrupts:
	default:
	}
	s.responseID = responseID
	if responseID != "" {
		if _, exists := s.knownResponseIDs[responseID]; !exists {
			if len(s.knownOrder) >= responsesLocalInterruptMaxKnownIDs {
				oldest := s.knownOrder[0]
				s.knownOrder = s.knownOrder[1:]
				delete(s.knownResponseIDs, oldest)
			}
			s.knownResponseIDs[responseID] = struct{}{}
			s.knownOrder = append(s.knownOrder, responseID)
		}
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
		// Each response queues at most one cancellation; non-blocking send prevents deadlock.
		s.responseID = ""
		select {
		case s.interrupts <- responseID:
		default:
		}
	}
	return true
}

func (s *responsesLocalInterrupt) interruptsChan() <-chan string {
	if s == nil {
		return nil
	}
	return s.interrupts
}
