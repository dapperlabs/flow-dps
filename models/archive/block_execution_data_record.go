package archive

import "github.com/onflow/flow-go/module/executiondatasync/execution_data"

type BlockExecutionDataRecord struct {
	ExecutionData *execution_data.BlockExecutionData
	Height        uint64
}
