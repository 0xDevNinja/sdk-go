package internal

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/api/workflowservicemock/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// -- BASIC TESTS ----------------------------------------------------

// -- DATA --

// temperature keeps its state private and transfers as a float64.
type temperature struct {
	TransferTypeConvertible[temperature, float64, temperatureConverter]
	kelvin float64
}

type temperatureConverter struct{}

func (temperatureConverter) ToTransfer(_ context.Context, t *temperature) (*float64, error) {
	return &t.kelvin, nil
}

func (temperatureConverter) FromTransfer(_ context.Context, kelvin *float64, t *temperature) error {
	t.kelvin = *kelvin
	return nil
}

func (temperatureConverter) ToTransferWithWorkflowContext(_ Context, t *temperature) (*float64, error) {
	return &t.kelvin, nil
}

func (temperatureConverter) FromTransferWithWorkflowContext(_ Context, kelvin *float64, t *temperature) error {
	t.kelvin = *kelvin
	return nil
}

// unencodable always returns an error during transfer-type-encoding.
type unencodable struct {
	TransferTypeConvertible[unencodable, string, unencodableConverter]
}

var errNoEncoding = errors.New("cannot encode")

type unencodableConverter struct{}

func (unencodableConverter) ToTransfer(context.Context, *unencodable) (*string, error) {
	return nil, errNoEncoding
}
func (unencodableConverter) FromTransfer(context.Context, *string, *unencodable) error { return nil }

func (unencodableConverter) ToTransferWithWorkflowContext(Context, *unencodable) (*string, error) {
	return nil, errNoEncoding
}

func (unencodableConverter) FromTransferWithWorkflowContext(Context, *string, *unencodable) error {
	return nil
}

// undecodable always returns an error during transfer-type-decoding.
type undecodable struct {
	TransferTypeConvertible[undecodable, string, undecodableConverter]
}

var errNoDecoding = errors.New("cannot decode")

type undecodableConverter struct{}

func (undecodableConverter) ToTransfer(context.Context, *undecodable) (*string, error) {
	encoded := "encoded"
	return &encoded, nil
}

func (undecodableConverter) FromTransfer(context.Context, *string, *undecodable) error {
	return errNoDecoding
}

func (undecodableConverter) ToTransferWithWorkflowContext(Context, *undecodable) (*string, error) {
	encoded := "encoded"
	return &encoded, nil
}

func (undecodableConverter) FromTransferWithWorkflowContext(Context, *string, *undecodable) error {
	return errNoDecoding
}

// contextualString transfers with a label from the current execution context.
type contextualString struct {
	TransferTypeConvertible[contextualString, string, contextualStringConverter]
	Value string
}

type transferContextKey struct{}
type contextualStringConverter struct{}

func (contextualStringConverter) ToTransfer(ctx context.Context, value *contextualString) (*string, error) {
	label, _ := ctx.Value(transferContextKey{}).(string)
	transfer := "go:" + label + ":" + value.Value
	return &transfer, nil
}
func (contextualStringConverter) FromTransfer(ctx context.Context, transfer *string, value *contextualString) error {
	label, _ := ctx.Value(transferContextKey{}).(string)
	if !strings.HasPrefix(*transfer, "go:"+label+":") {
		return fmt.Errorf("unexpected Go context label %q for %q", label, *transfer)
	}
	value.Value = strings.Split(*transfer, ":")[2]
	return nil
}
func (contextualStringConverter) ToTransferWithWorkflowContext(ctx Context, value *contextualString) (*string, error) {
	label, _ := ctx.Value(transferContextKey{}).(string)
	transfer := "wf:" + label + ":" + value.Value
	return &transfer, nil
}
func (contextualStringConverter) FromTransferWithWorkflowContext(ctx Context, transfer *string, value *contextualString) error {
	label, _ := ctx.Value(transferContextKey{}).(string)
	if !strings.HasPrefix(*transfer, "wf:"+label+":") {
		return fmt.Errorf("unexpected workflow context label %q for %q", label, *transfer)
	}
	value.Value = strings.Split(*transfer, ":")[2]
	return nil
}

// transferEnvelope is a struct that contains a transfer-convertible field,
// but the struct itself has no transfer type converter.
type transferEnvelope struct{ Value contextualString }

// -- TESTS --

func TestTransferAwareDataConverter_PayloadRoundTrip(t *testing.T) {
	t.Parallel()
	dc := defaultTransferAwareDataConverter

	t.Run("scalar transfer values", func(t *testing.T) {
		values := make([]temperature, 10)
		for i := range values {
			values[i] = temperature{kelvin: rand.Float64() * 1_000}
		}

		for _, value := range values {
			payload, err := dc.ToPayload(value)
			require.NoError(t, err)

			var got temperature
			require.NoError(t, dc.FromPayload(payload, &got))
			require.Equal(t, value, got)
		}
	})

	t.Run("values without a transfer type converter", func(t *testing.T) {
		values := make([]string, 10)
		for i := range values {
			values[i] = "plain-" + strconv.FormatUint(rand.Uint64(), 10)
		}

		for _, value := range values {
			payload, err := dc.ToPayload(value)
			require.NoError(t, err)

			want, err := converter.GetDefaultDataConverter().ToPayload(value)
			require.NoError(t, err)
			require.Equal(t, want.GetData(), payload.GetData())

			var got string
			require.NoError(t, dc.FromPayload(payload, &got))
			require.Equal(t, value, got)
		}
	})
}

func TestTransferAwareDataConverter_PointerValueRoundTrip(t *testing.T) {
	t.Parallel()
	dc := defaultTransferAwareDataConverter
	want := &temperature{kelvin: 300}

	payload, err := dc.ToPayload(want)
	require.NoError(t, err)

	var got temperature
	require.NoError(t, dc.FromPayload(payload, &got))
	require.Equal(t, *want, got)
}

func TestTransferAwareDataConverter_DoublePointerRoundTrip(t *testing.T) {
	dc := makeTransferAware(converter.GetDefaultDataConverter())
	in := &temperature{kelvin: 300}
	payloads, err := dc.ToPayloads(in)
	require.NoError(t, err)

	var out *temperature
	require.NoError(t, dc.FromPayloads(payloads, &out))
	require.Equal(t, in, out)
}

func TestTransferAwareDataConverter_DoublePointerDecoding(t *testing.T) {
	t.Parallel()
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprintf("batch=%v", batch), func(t *testing.T) {
			decode := func(dc *transferAwareDataConverter, payload *commonpb.Payload, destination any) error {
				if batch {
					return dc.FromPayloads(&commonpb.Payloads{Payloads: []*commonpb.Payload{payload}}, destination)
				}
				return dc.FromPayload(payload, destination)
			}
			for _, populated := range []bool{false, true} {
				t.Run(fmt.Sprintf("populated=%v", populated), func(t *testing.T) {
					parent := converter.GetDefaultDataConverter()
					payload, err := parent.ToPayload(300.0)
					require.NoError(t, err)
					var out *temperature
					if populated {
						out = &temperature{kelvin: 99}
					}
					original := out
					require.NoError(t, decode(makeTransferAware(parent), payload, &out))
					require.Equal(t, &temperature{kelvin: 300}, out)
					if populated {
						require.Same(t, original, out)
					}
				})
			}
			t.Run("plain pointer", func(t *testing.T) {
				parent := converter.GetDefaultDataConverter()
				payload, err := parent.ToPayload("plain")
				require.NoError(t, err)
				var out *string
				require.NoError(t, decode(makeTransferAware(parent), payload, &out))
				require.Equal(t, new("plain"), out)
			})
			t.Run("nil transfer value", func(t *testing.T) {
				parent := converter.GetDefaultDataConverter()
				payload, err := parent.ToPayload(nil)
				require.NoError(t, err)
				var out *temperature
				require.NoError(t, decode(makeTransferAware(parent), payload, &out))
				require.Equal(t, &temperature{}, out)
			})
			t.Run("invalid transfer value", func(t *testing.T) {
				parent := converter.GetDefaultDataConverter()
				payload, err := parent.ToPayload("not a temperature")
				require.NoError(t, err)
				var out *temperature
				require.Error(t, decode(makeTransferAware(parent), payload, &out))
			})
		})
	}
}

func TestTransferAwareDataConverter_PayloadsRoundTrip(t *testing.T) {
	t.Parallel()
	dc := defaultTransferAwareDataConverter

	t.Run("scalar transfer values", func(t *testing.T) {
		values := make([]temperature, 10)
		valuePtrs := make([]any, len(values))
		got := make([]temperature, len(values))
		for i := range values {
			values[i] = temperature{kelvin: rand.Float64() * 1_000}
			valuePtrs[i] = &got[i]
		}

		payloads, err := dc.ToPayloads(sliceToAny(values)...)
		require.NoError(t, err)
		require.NoError(t, dc.FromPayloads(payloads, valuePtrs...))
		require.Equal(t, values, got)
	})

	t.Run("values without a transfer type converter", func(t *testing.T) {
		values := make([]string, 10)
		valuePtrs := make([]any, len(values))
		got := make([]string, len(values))
		for i := range values {
			values[i] = "plain-" + strconv.FormatUint(rand.Uint64(), 10)
			valuePtrs[i] = &got[i]
		}

		payloads, err := dc.ToPayloads(sliceToAny(values)...)
		require.NoError(t, err)
		require.NoError(t, dc.FromPayloads(payloads, valuePtrs...))
		require.Equal(t, values, got)
	})
}

func sliceToAny[T any](values []T) []any {
	result := make([]any, len(values))
	for i := range values {
		result[i] = values[i]
	}
	return result
}

func TestTransferAwareDataConverter_MatchesParentForPlainValues(t *testing.T) {
	t.Parallel()
	parent := converter.GetDefaultDataConverter()
	dc := makeTransferAware(parent)

	requireSamePayloads := func(t *testing.T, want, got *commonpb.Payloads) {
		t.Helper()
		require.Len(t, got.GetPayloads(), len(want.GetPayloads()))
		for i, wantPayload := range want.GetPayloads() {
			require.Equal(t, wantPayload.GetData(), got.GetPayloads()[i].GetData(), "payload %d data", i)
			require.Equal(t, wantPayload.GetMetadata(), got.GetPayloads()[i].GetMetadata(), "payload %d metadata", i)
		}
	}

	t.Run("no values", func(t *testing.T) {
		got, err := dc.ToPayloads()
		require.NoError(t, err)
		want, err := parent.ToPayloads()
		require.NoError(t, err)
		require.Equal(t, want, got)
	})

	t.Run("only plain values", func(t *testing.T) {
		values := []any{"plain", 42, []string{"a", "b"}, nil}
		got, err := dc.ToPayloads(values...)
		require.NoError(t, err)
		want, err := parent.ToPayloads(values...)
		require.NoError(t, err)
		requireSamePayloads(t, want, got)
	})

	// Plain values keep their place and their encoding even when a transfer
	// value sits next to them.
	t.Run("plain values mixed with transfer values", func(t *testing.T) {
		got, err := dc.ToPayloads("plain", temperature{kelvin: 300}, 42, temperature{kelvin: 275}, nil)
		require.NoError(t, err)
		want, err := parent.ToPayloads("plain", 300.0, 42, 275.0, nil)
		require.NoError(t, err)
		requireSamePayloads(t, want, got)
	})

	t.Run("decoding only plain values", func(t *testing.T) {
		payloads, err := dc.ToPayloads("plain", 42)
		require.NoError(t, err)

		var (
			gotString string
			gotInt    int
		)
		require.NoError(t, dc.FromPayloads(payloads, &gotString, &gotInt))
		require.Equal(t, "plain", gotString)
		require.Equal(t, 42, gotInt)
	})
}

func TestTransferAwareDataConverter_ConversionErrors(t *testing.T) {
	t.Parallel()
	dc := defaultTransferAwareDataConverter

	t.Run("encoding one value", func(t *testing.T) {
		_, err := dc.ToPayload(unencodable{})
		require.ErrorIs(t, err, errNoEncoding)
	})

	t.Run("encoding a list", func(t *testing.T) {
		_, err := dc.ToPayloads("plain", unencodable{})
		require.ErrorIs(t, err, errNoEncoding)
		require.Contains(t, err.Error(), "values[1]")
	})

	t.Run("decoding one value", func(t *testing.T) {
		payload, err := dc.ToPayload(undecodable{})
		require.NoError(t, err)
		require.ErrorIs(t, dc.FromPayload(payload, &undecodable{}), errNoDecoding)
	})

	t.Run("decoding a list", func(t *testing.T) {
		payloads, err := dc.ToPayloads("plain", undecodable{})
		require.NoError(t, err)

		var got string
		err = dc.FromPayloads(payloads, &got, &undecodable{})
		require.ErrorIs(t, err, errNoDecoding)
		require.Contains(t, err.Error(), "payload item 1")
	})
}

func TestTransferAwareDataConverter_ContextDelegation(t *testing.T) {
	t.Parallel()

	t.Run("context-aware parent", func(t *testing.T) {
		dc := makeTransferAware(NewContextAwareDataConverter(converter.GetDefaultDataConverter()))

		ctx := context.WithValue(t.Context(), ContextAwareDataConverterContextKey, "300")
		masked := WithContext(ctx, dc)
		require.NotSame(t, dc, masked)

		payload, err := masked.ToPayload(temperature{kelvin: 300})
		require.NoError(t, err)
		require.Equal(t, "?", string(payload.GetData()))
	})

	// Even when the parent has no use for a context, we hold on to it: transfer
	// converters may still want it.
	t.Run("parent that is not context aware", func(t *testing.T) {
		dc := defaultTransferAwareDataConverter
		require.NotSame(t, dc, WithContext(t.Context(), dc))
		require.NotSame(t, dc, WithWorkflowContext(Background(), dc))
		// Serialization contexts are only forwarded, so there is nothing to keep.
		require.Same(t, dc, dc.WithSerializationContext(converter.WorkflowSerializationContext{}))
	})
}

func TestTransferAwareDataConverter_ConversionContext(t *testing.T) {
	t.Parallel()
	parent := converter.GetDefaultDataConverter()

	// requireRoundTrip checks that contextualString values are encoded with wantPrefix
	// and decoded back to their original form.
	requireRoundTrip := func(t *testing.T, dc converter.DataConverter, wantPrefix string) {
		t.Helper()

		payload, err := dc.ToPayload(contextualString{Value: "value"})
		require.NoError(t, err)
		want, err := parent.ToPayload(wantPrefix + "value")
		require.NoError(t, err)
		require.Equal(t, want.GetData(), payload.GetData())

		var got contextualString
		require.NoError(t, dc.FromPayload(payload, &got))
		require.Equal(t, contextualString{Value: "value"}, got)

		payloads, err := dc.ToPayloads(contextualString{Value: "one"}, contextualString{Value: "two"})
		require.NoError(t, err)
		wants, err := parent.ToPayloads(wantPrefix+"one", wantPrefix+"two")
		require.NoError(t, err)
		require.Equal(t, wants.GetPayloads()[0].GetData(), payloads.GetPayloads()[0].GetData())
		require.Equal(t, wants.GetPayloads()[1].GetData(), payloads.GetPayloads()[1].GetData())

		var gotOne, gotTwo contextualString
		require.NoError(t, dc.FromPayloads(payloads, &gotOne, &gotTwo))
		require.Equal(t, contextualString{Value: "one"}, gotOne)
		require.Equal(t, contextualString{Value: "two"}, gotTwo)
	}

	t.Run("base", func(t *testing.T) {
		requireRoundTrip(t, makeTransferAware(nil), "go::")
	})
	t.Run("Go context", func(t *testing.T) {
		ctx := context.WithValue(t.Context(), transferContextKey{}, "client")
		requireRoundTrip(t, makeTransferAware(nil).WithContext(ctx), "go:client:")
	})
	t.Run("workflow context", func(t *testing.T) {
		ctx := WithValue(Background(), transferContextKey{}, "workflow")
		requireRoundTrip(t, defaultTransferAwareDataConverter.WithWorkflowContext(ctx), "wf:workflow:")
	})
}

// -- SDK INTEGRATION TESTS -----------------------------------------------------------

func newTransferTestClient(t *testing.T, dc converter.DataConverter) (*workflowservicemock.MockWorkflowServiceClient, *WorkflowClient) {
	t.Helper()
	service := workflowservicemock.NewMockWorkflowServiceClient(gomock.NewController(t))
	service.EXPECT().GetSystemInfo(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&workflowservice.GetSystemInfoResponse{}, nil).AnyTimes()
	client := NewServiceClient(service, nil, ClientOptions{
		Namespace: "transfer-test", DataConverter: dc,
	})
	return service, client
}

func TestTransferTypesMockClientWorkflowInput(t *testing.T) {
	for _, tt := range []struct {
		name     string
		workflow any
		args     []any
		wireArgs []any
	}{
		{
			name:     "workflow args use transfer type converters when available",
			workflow: func(Context, string, temperature, temperature, transferEnvelope) error { return nil },
			args:     []any{"plain", temperature{kelvin: 300}, temperature{kelvin: 275}, transferEnvelope{Value: contextualString{Value: "value"}}},
			wireArgs: []any{"plain", 300.0, 275.0, map[string]any{"Value": map[string]string{"Value": "value"}}},
		},
		{
			name:     "nested values are not transfer-converted",
			workflow: func(Context, transferEnvelope, []contextualString, map[string]contextualString) error { return nil },
			args:     []any{transferEnvelope{Value: contextualString{Value: "value"}}, []contextualString{{Value: "value"}}, map[string]contextualString{"key": {Value: "value"}}},
			wireArgs: []any{map[string]any{"Value": map[string]string{"Value": "value"}}, []map[string]string{{"Value": "value"}}, map[string]any{"key": map[string]string{"Value": "value"}}},
		},
		{
			name:     "client context reaches transfer type converter",
			workflow: func(Context, contextualString) error { return nil },
			args:     []any{contextualString{Value: "value"}},
			wireArgs: []any{"go:client:value"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dc := converter.GetDefaultDataConverter()
			service, client := newTransferTestClient(t, dc)
			want, err := dc.ToPayloads(tt.wireArgs...)
			require.NoError(t, err)
			service.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, req *workflowservice.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflowservice.StartWorkflowExecutionResponse, error) {
					require.True(t, proto.Equal(want, req.Input), "got input %v, want %v", req.Input, want)
					return &workflowservice.StartWorkflowExecutionResponse{RunId: "run-1"}, nil
				})

			ctx := context.WithValue(t.Context(), transferContextKey{}, "client")
			_, err = client.ExecuteWorkflow(ctx, StartWorkflowOptions{
				ID: "workflow-1", TaskQueue: "transfer-test",
			}, tt.workflow, tt.args...)
			require.NoError(t, err)
		})
	}
}

func TestTransferTypesMockClientWorkflowResult(t *testing.T) {
	for _, tt := range []struct {
		name      string
		wireValue any
		resultPtr any
		want      any
	}{
		{"requested model type", 300.0, new(temperature), &temperature{kelvin: 300}},
		{"plain destination", "value", new(string), new("value")},
		{"any destination", 300.0, new(any), new(any(300.0))},
		{"nested values", map[string]any{"Value": map[string]string{"Value": "value"}}, new(transferEnvelope), &transferEnvelope{Value: contextualString{Value: "value"}}},
		{"result decoding without execution context", "go::value", new(contextualString), new(contextualString{Value: "value"})},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dc := converter.GetDefaultDataConverter()
			service, client := newTransferTestClient(t, dc)
			payloads, err := dc.ToPayloads(tt.wireValue)
			require.NoError(t, err)
			service.EXPECT().GetWorkflowExecutionHistory(gomock.Any(), gomock.Any(), gomock.Any()).
				Return(&workflowservice.GetWorkflowExecutionHistoryResponse{
					History: &historypb.History{Events: []*historypb.HistoryEvent{{
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
						Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
							WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{Result: payloads},
						},
					}}},
				}, nil)

			ctx := context.WithValue(t.Context(), transferContextKey{}, "client")
			err = client.GetWorkflow(ctx, "workflow-1", "run-1").Get(ctx, tt.resultPtr)
			require.NoError(t, err)
			require.Equal(t, tt.want, tt.resultPtr)
		})
	}
}

// transferExecution's transfer type includes its unexported fields. When encoded with a data
// converter that isn't transfer-aware, the fields disappear.
type transferExecution struct {
	TransferTypeConvertible[transferExecution, commonpb.WorkflowExecution, transferExecutionConverter]
	workflowID, runID string
}

type transferExecutionConverter struct{}

func (transferExecutionConverter) ToTransfer(_ context.Context, value *transferExecution) (*commonpb.WorkflowExecution, error) {
	return &commonpb.WorkflowExecution{WorkflowId: value.workflowID, RunId: value.runID}, nil
}

func (transferExecutionConverter) FromTransfer(_ context.Context, value *commonpb.WorkflowExecution, result *transferExecution) error {
	*result = transferExecution{workflowID: value.GetWorkflowId(), runID: value.GetRunId()}
	return nil
}

func (transferExecutionConverter) ToTransferWithWorkflowContext(_ Context, value *transferExecution) (*commonpb.WorkflowExecution, error) {
	return &commonpb.WorkflowExecution{WorkflowId: value.workflowID, RunId: value.runID}, nil
}

func (transferExecutionConverter) FromTransferWithWorkflowContext(_ Context, value *commonpb.WorkflowExecution, result *transferExecution) error {
	*result = transferExecution{workflowID: value.GetWorkflowId(), runID: value.GetRunId()}
	return nil
}

func TestTransferTypesWorkflowTestEnvironmentRoundTrip(t *testing.T) {
	dc := converter.GetDefaultDataConverter()
	model := transferExecution{workflowID: "workflow-1", runID: "run-1"}

	t.Run("round trip through test workflow environment", func(t *testing.T) {
		var suite WorkflowTestSuite
		env := suite.NewTestWorkflowEnvironment()
		env.SetDataConverter(dc)
		env.ExecuteWorkflow(func(_ Context, input transferExecution) (transferExecution, error) {
			input.runID += ":completed"
			return input, nil
		}, model)
		require.True(t, env.IsWorkflowCompleted())
		require.NoError(t, env.GetWorkflowError())
		var got transferExecution
		require.NoError(t, env.GetWorkflowResult(&got))
		require.Equal(t, transferExecution{workflowID: model.workflowID, runID: model.runID + ":completed"}, got)
	})
}

func TestTransferTypesWorkflowTestEnvironmentWorkflowRoundTrip(t *testing.T) {
	for _, tt := range []struct {
		name      string
		workflow  any
		input     any
		resultPtr any
		want      any
	}{
		{
			name: "model input and result",
			workflow: func(_ Context, input temperature) (temperature, error) {
				return temperature{kelvin: input.kelvin + 10}, nil
			},
			input: temperature{kelvin: 300}, resultPtr: new(temperature), want: &temperature{kelvin: 310},
		},
		{
			name: "nested field is not transfer converted",
			workflow: func(_ Context, input transferEnvelope) (transferEnvelope, error) {
				return input, nil
			},
			input: transferEnvelope{Value: contextualString{Value: "value"}}, resultPtr: new(transferEnvelope), want: &transferEnvelope{Value: contextualString{Value: "value"}},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var suite WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			env.ExecuteWorkflow(tt.workflow, tt.input)
			require.True(t, env.IsWorkflowCompleted())
			require.NoError(t, env.GetWorkflowError())
			require.NoError(t, env.GetWorkflowResult(tt.resultPtr))
			require.Equal(t, tt.want, tt.resultPtr)
		})
	}
}

func TestTransferTypesWorkflowTestEnvironmentActivityRoundTrip(t *testing.T) {
	activity := func(_ context.Context, input temperature, increment float64) (temperature, error) {
		return temperature{kelvin: input.kelvin + increment}, nil
	}
	for _, local := range []bool{false, true} {
		t.Run(fmt.Sprintf("local=%t", local), func(t *testing.T) {
			var suite WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			env.RegisterActivity(activity)
			env.ExecuteWorkflow(func(ctx Context) (float64, error) {
				ctx = WithActivityOptions(ctx, ActivityOptions{
					StartToCloseTimeout: time.Minute, RetryPolicy: &RetryPolicy{MaximumAttempts: 1},
				})
				ctx = WithLocalActivityOptions(ctx, LocalActivityOptions{
					StartToCloseTimeout: time.Minute, RetryPolicy: &RetryPolicy{MaximumAttempts: 1},
				})
				var future Future
				if local {
					future = ExecuteLocalActivity(ctx, activity, temperature{kelvin: 300}, 42.0)
				} else {
					future = ExecuteActivity(ctx, activity, temperature{kelvin: 300}, 42.0)
				}
				var got temperature
				if err := future.Get(ctx, &got); err != nil {
					return 0, err
				}
				return got.kelvin, nil
			})
			require.True(t, env.IsWorkflowCompleted())
			require.NoError(t, env.GetWorkflowError())
			var got float64
			require.NoError(t, env.GetWorkflowResult(&got))
			require.Equal(t, 342.0, got)
		})
	}
}

func TestTransferTypesWorkflowTestEnvironmentExecutionConversionContext(t *testing.T) {
	t.Run("activity callback", func(t *testing.T) {
		var suite WorkflowTestSuite
		env := suite.NewTestWorkflowEnvironment()
		env.SetWorkerOptions(WorkerOptions{
			BackgroundActivityContext: context.WithValue(t.Context(), transferContextKey{}, "activity"),
		})
		activity := func(context.Context) (contextualString, error) {
			return contextualString{Value: "value"}, nil
		}
		env.RegisterActivity(activity)
		env.ExecuteWorkflow(func(ctx Context) (string, error) {
			ctx = WithActivityOptions(ctx, ActivityOptions{
				StartToCloseTimeout: time.Minute, RetryPolicy: &RetryPolicy{MaximumAttempts: 1},
			})
			var got string
			err := ExecuteActivity(ctx, activity).Get(ctx, &got)
			return got, err
		})
		require.True(t, env.IsWorkflowCompleted())
		require.NoError(t, env.GetWorkflowError())
		var got string
		require.NoError(t, env.GetWorkflowResult(&got))
		require.Equal(t, "go:activity:value", got)
	})

	t.Run("workflow decoding callback", func(t *testing.T) {
		var suite WorkflowTestSuite
		env := suite.NewTestWorkflowEnvironment()
		activity := func(context.Context) (string, error) {
			return "wf:workflow:value", nil
		}
		env.RegisterActivity(activity)
		env.ExecuteWorkflow(func(ctx Context) (string, error) {
			ctx = WithValue(ctx, transferContextKey{}, "workflow")
			ctx = WithActivityOptions(ctx, ActivityOptions{
				StartToCloseTimeout: time.Minute, RetryPolicy: &RetryPolicy{MaximumAttempts: 1},
			})
			var got contextualString
			err := ExecuteActivity(ctx, activity).Get(ctx, &got)
			return got.Value, err
		})
		require.True(t, env.IsWorkflowCompleted())
		require.NoError(t, env.GetWorkflowError())
		var got string
		require.NoError(t, env.GetWorkflowResult(&got))
		require.Equal(t, "value", got)
	})
}

func TestTransferTypes_DataConverterWrapping(t *testing.T) {
	value := temperature{kelvin: 300}
	payloads, err := defaultTransferAwareDataConverter.ToPayloads(value)
	require.NoError(t, err)

	t.Run("workflow replayer", func(t *testing.T) {
		replayer, err := NewWorkflowReplayer(WorkflowReplayerOptions{
			DataConverter: converter.GetDefaultDataConverter(),
		})
		require.NoError(t, err)
		replayer.workflowExecutionResults["workflow-1"] = payloads

		var got temperature
		require.NoError(t, replayer.GetWorkflowResult("workflow-1", &got))
		require.Equal(t, value, got)
	})

	t.Run("workflow context", func(t *testing.T) {
		ctx := WithDataConverter(Background(), converter.GetDefaultDataConverter())
		options := getWorkflowEnvOptions(ctx)
		require.Same(t, options.DataConverter, options.RootDataConverter)
		dc := GetDataConverterFromWorkflowContext(ctx)

		var got temperature
		require.NoError(t, dc.FromPayloads(payloads, &got))
		require.Equal(t, value, got)
	})

	t.Run("encoded values", func(t *testing.T) {
		var encodedValue temperature
		require.NoError(t, newEncodedValue(payloads, converter.GetDefaultDataConverter()).Get(&encodedValue))
		require.Equal(t, value, encodedValue)

		var encodedValues temperature
		require.NoError(t, newEncodedValues(payloads, converter.GetDefaultDataConverter()).Get(&encodedValues))
		require.Equal(t, value, encodedValues)
	})
}

func (s *TaskHandlersTestSuite) TestTransferTypesWorkflowBoundaryInvalidMarker() {
	for _, test := range []struct {
		name     string
		workflow any
	}{
		{"encode", func(ctx Context) error {
			ctx = WithActivityOptions(ctx, ActivityOptions{StartToCloseTimeout: time.Minute})
			return ExecuteActivity(ctx, "unused", wrongMarker{}).Get(ctx, nil)
		}},
		{"decode", func(Context, wrongMarker) error { return nil }},
	} {
		s.Run(test.name, func() {
			s.registry.RegisterWorkflowWithOptions(test.workflow, RegisterWorkflowOptions{Name: test.name})
			input, err := converter.GetDefaultDataConverter().ToPayloads("transfer")
			s.Require().NoError(err)
			events := []*historypb.HistoryEvent{
				createTestEventWorkflowExecutionStarted(1, &historypb.WorkflowExecutionStartedEventAttributes{
					Input: input, TaskQueue: &taskqueuepb.TaskQueue{Name: testWorkflowTaskTaskqueue},
				}),
				createTestEventWorkflowTaskScheduled(2, &historypb.WorkflowTaskScheduledEventAttributes{}),
				createTestEventWorkflowTaskStarted(3),
			}
			params := s.getTestWorkerExecutionParams()
			s.Require().Equal(BlockWorkflow, params.WorkflowPanicPolicy)
			handler := newWorkflowTaskHandler(params, nil, s.registry)
			task := workflowTask{task: createWorkflowTask(events, 3, test.name)}
			ctx := s.mustWorkflowContextImpl(&task, handler)
			completion, err := handler.ProcessWorkflowTask(&task, ctx, nil)
			ctx.Unlock(err)
			s.Require().IsType(&workflowPanicError{}, err)
			s.ErrorContains(err, "must match enclosing struct")
			s.Nil(completion)
		})
	}
}

var (
	_ TransferTypeConverter[temperature, float64]                          = temperatureConverter{}
	_ TransferTypeConverter[unencodable, string]                           = unencodableConverter{}
	_ TransferTypeConverter[undecodable, string]                           = undecodableConverter{}
	_ TransferTypeConverter[contextualString, string]                      = contextualStringConverter{}
	_ TransferTypeConverter[transferExecution, commonpb.WorkflowExecution] = transferExecutionConverter{}
)

// contextDispatchModel records the context delivered by the required methods.
type contextDispatchModel struct {
	TransferTypeConvertible[contextDispatchModel, string, contextDispatchConverter]
	goContext       context.Context
	workflowContext Context
}

type contextDispatchConverter struct{}

var _ TransferTypeConverter[contextDispatchModel, string] = contextDispatchConverter{}

func (contextDispatchConverter) ToTransfer(ctx context.Context, value *contextDispatchModel) (*string, error) {
	if ctx == nil || ctx != value.goContext {
		return nil, fmt.Errorf("unexpected Go context: %v", ctx)
	}
	return new("go"), nil
}

func (contextDispatchConverter) FromTransfer(ctx context.Context, _ *string, value *contextDispatchModel) error {
	value.goContext = ctx
	return nil
}

func (contextDispatchConverter) ToTransferWithWorkflowContext(ctx Context, value *contextDispatchModel) (*string, error) {
	if ctx == nil || ctx != value.workflowContext {
		return nil, fmt.Errorf("unexpected workflow context: %v", ctx)
	}
	return new("workflow"), nil
}

func (contextDispatchConverter) FromTransferWithWorkflowContext(ctx Context, _ *string, value *contextDispatchModel) error {
	value.workflowContext = ctx
	return nil
}

func TestTransferTypeConvertibleRequiredContextDispatch(t *testing.T) {
	base := makeTransferAware(nil)
	goCtx := t.Context()
	wfCtx := WithValue(Background(), transferContextKey{}, "workflow")

	for _, tt := range []struct {
		name string
		dc   converter.DataConverter
		want string
	}{
		{"Background", base, "go"},
		{"Go context", base.WithContext(goCtx), "go"},
		{"workflow context", base.WithWorkflowContext(wfCtx), "workflow"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, batch := range []bool{false, true} {
				payload, err := converter.GetDefaultDataConverter().ToPayload(tt.want)
				require.NoError(t, err)
				var got contextDispatchModel
				if batch {
					err = tt.dc.FromPayloads(&commonpb.Payloads{Payloads: []*commonpb.Payload{payload}}, &got)
				} else {
					err = tt.dc.FromPayload(payload, &got)
				}
				require.NoError(t, err)
				if tt.want == "workflow" {
					require.Equal(t, wfCtx, got.workflowContext)
					require.Nil(t, got.goContext)
				} else {
					require.NotNil(t, got.goContext)
					require.Nil(t, got.workflowContext)
					if tt.name == "Go context" {
						require.Equal(t, goCtx, got.goContext)
					} else {
						require.Nil(t, got.goContext.Done())
						require.NoError(t, got.goContext.Err())
						_, hasDeadline := got.goContext.Deadline()
						require.False(t, hasDeadline)
					}
				}
				if batch {
					payloads, err := tt.dc.ToPayloads(got)
					require.NoError(t, err)
					require.Equal(t, payload, payloads.Payloads[0])
				} else {
					encoded, err := tt.dc.ToPayload(got)
					require.NoError(t, err)
					require.Equal(t, payload, encoded)
				}
			}
		})
	}
}
