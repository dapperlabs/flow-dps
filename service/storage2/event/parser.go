package event

// https://github.com/onflow/event-indexer/blob/main/internal/parser/event.go
// Author: @peterargue

import (
	"fmt"
	"strings"
)

type ParsedEvent struct {
	Type         string
	Address      string
	ContractName string
	EventName    string
}

func ParseEvent(eventType string) (*ParsedEvent, error) {
	parts := strings.Split(eventType, ".")
	if len(parts) == 2 {
		if parts[0] != "flow" {
			return nil, fmt.Errorf("invalid flow event type")
		}

		return &ParsedEvent{
			Type:         eventType,
			ContractName: parts[0],
			EventName:    parts[1],
		}, nil
	}

	if len(parts) != 4 || parts[0] != "A" {
		return nil, fmt.Errorf("invalid account event type")
	}

	return &ParsedEvent{
		Type:         eventType,
		Address:      parts[1],
		ContractName: parts[2],
		EventName:    parts[3],
	}, nil
}

func (e *ParsedEvent) IsFlowEvent() bool {
	return e.ContractName == "flow" && e.Address == ""
}

func (e *ParsedEvent) AddressPrefix() string {
	if e.IsFlowEvent() {
		return ""
	}
	return fmt.Sprintf("A.%s", e.Address)
}

func (e *ParsedEvent) ContractPrefix() string {
	if e.IsFlowEvent() {
		return e.ContractName
	}
	return fmt.Sprintf("A.%s.%s", e.Address, e.ContractName)
}
