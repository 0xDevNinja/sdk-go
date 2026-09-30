package workflow_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

type transferAccount struct {
	workflow.TransferTypeConvertible[transferAccount, string, accountConverter]
	name string
}

type accountConverter struct{}

func (accountConverter) ToTransfer(_ context.Context, account *transferAccount) (*string, error) {
	return &account.name, nil
}

func (accountConverter) FromTransfer(_ context.Context, name *string, account *transferAccount) error {
	account.name = *name
	return nil
}

func (accountConverter) ToTransferWithWorkflowContext(_ workflow.Context, account *transferAccount) (*string, error) {
	return &account.name, nil
}

func (accountConverter) FromTransferWithWorkflowContext(_ workflow.Context, name *string, account *transferAccount) error {
	account.name = *name
	return nil
}

var _ workflow.TransferTypeConverter[transferAccount, string] = accountConverter{}

func TestTransferTypeConvertiblePublicAPI(t *testing.T) {
	require.Zero(t, reflect.TypeFor[workflow.TransferTypeConvertible[transferAccount, string, accountConverter]]().Size())
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(func(_ workflow.Context, account transferAccount) (transferAccount, error) {
		account.name += " returned"
		return account, nil
	}, transferAccount{name: "example"})
	require.NoError(t, env.GetWorkflowError())
	var result *transferAccount
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "example returned", result.name)
}

// Embed the marker directly in the model. The converter accesses private model
// fields and supplies the representation serialized in workflow payloads.
func ExampleTransferTypeConvertible() {
	account := transferAccount{name: "example"}
	_ = account // Pass account as a workflow or activity argument or result.
}

type wrongTransferAccount struct {
	workflow.TransferTypeConvertible[transferAccount, string, accountConverter]
}

type inheritedTransferAccount struct {
	transferAccount
}

func TestTransferTypeConvertiblePublicInheritedModel(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(func(workflow.Context) (inheritedTransferAccount, error) {
		return inheritedTransferAccount{}, nil
	})
	require.ErrorContains(t, env.GetWorkflowError(), "must match enclosing struct")
}

type pointerTransferAccount struct {
	*workflow.TransferTypeConvertible[pointerTransferAccount, string, pointerAccountConverter]
	name string
}

type pointerAccountConverter struct{}

var _ workflow.TransferTypeConverter[pointerTransferAccount, string] = pointerAccountConverter{}

func (pointerAccountConverter) ToTransfer(_ context.Context, account *pointerTransferAccount) (*string, error) {
	return &account.name, nil
}

func (pointerAccountConverter) FromTransfer(_ context.Context, name *string, account *pointerTransferAccount) error {
	account.name = *name
	return nil
}

func (pointerAccountConverter) ToTransferWithWorkflowContext(_ workflow.Context, account *pointerTransferAccount) (*string, error) {
	return &account.name, nil
}

func (pointerAccountConverter) FromTransferWithWorkflowContext(_ workflow.Context, name *string, account *pointerTransferAccount) error {
	account.name = *name
	return nil
}

func TestTransferTypeConvertiblePublicPointerMarker(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(func(_ workflow.Context, account pointerTransferAccount) (pointerTransferAccount, error) {
		account.name += " returned"
		return account, nil
	}, pointerTransferAccount{name: "example"})
	require.NoError(t, env.GetWorkflowError())
	var result *pointerTransferAccount
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "example returned", result.name)
	require.Nil(t, result.TransferTypeConvertible)
}

func TestTransferTypeConvertiblePublicWrongModel(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(func(workflow.Context) (wrongTransferAccount, error) {
		return wrongTransferAccount{}, nil
	})
	require.ErrorContains(t, env.GetWorkflowError(), "must match enclosing struct")
}
