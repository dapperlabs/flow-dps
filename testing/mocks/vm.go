package mocks

import (
	"testing"

	"github.com/onflow/flow-go/fvm"
	"github.com/onflow/flow-go/fvm/storage"
	"github.com/onflow/flow-go/fvm/storage/snapshot"
	"github.com/onflow/flow-go/model/flow"
)

type VirtualMachine struct {
	GetAccountFunc func(
		ctx fvm.Context,
		address flow.Address,
		v snapshot.StorageSnapshot,
	) (
		*flow.Account,
		error,
	)

	RunFunc func(
		ctx fvm.Context,
		proc fvm.Procedure,
		v snapshot.StorageSnapshot,
	) (
		*snapshot.ExecutionSnapshot,
		fvm.ProcedureOutput,
		error,
	)
}

func (v *VirtualMachine) NewExecutor(context fvm.Context, procedure fvm.Procedure, preparer storage.TransactionPreparer) fvm.ProcedureExecutor {
	//TODO implement me
	panic("implement me")
}

func BaselineVirtualMachine(t *testing.T) *VirtualMachine {
	t.Helper()

	vm := VirtualMachine{
		GetAccountFunc: func(
			ctx fvm.Context,
			address flow.Address,
			v snapshot.StorageSnapshot,
		) (
			*flow.Account,
			error,
		) {
			return &GenericAccount, nil
		},
		RunFunc: func(
			ctx fvm.Context,
			proc fvm.Procedure,
			v snapshot.StorageSnapshot,
		) (
			*snapshot.ExecutionSnapshot,
			fvm.ProcedureOutput,
			error,
		) {
			return &snapshot.ExecutionSnapshot{}, fvm.ProcedureOutput{}, nil
		},
	}

	return &vm
}

func (v *VirtualMachine) GetAccount(
	ctx fvm.Context,
	address flow.Address,
	view snapshot.StorageSnapshot,
) (
	*flow.Account,
	error,
) {
	return v.GetAccountFunc(ctx, address, view)
}

func (v *VirtualMachine) Run(
	ctx fvm.Context,
	proc fvm.Procedure,
	view snapshot.StorageSnapshot,
) (
	*snapshot.ExecutionSnapshot,
	fvm.ProcedureOutput,
	error,
) {
	return v.RunFunc(ctx, proc, view)
}
