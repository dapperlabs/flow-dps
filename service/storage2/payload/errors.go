package payload

import (
	"fmt"

	"github.com/onflow/flow-go/model/flow"
)

type ErrNotFound struct {
	RegisterID flow.RegisterID
	Height     uint64
}

func (e *ErrNotFound) Error() string {
	return fmt.Sprintf("key not found: %s at height <= %d", e.RegisterID.String(), e.Height)
}
