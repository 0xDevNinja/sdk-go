package internal

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
)

var (
	_ TransferTypeConverter[functionConverterMarker, string] = functionConverter(nil)
	_ TransferTypeConverter[multipleMarkers, int]            = integerConverter{}
	_ TransferTypeConverter[failingModel, string]            = failingConverter{}
)

type markedModel struct {
	TransferTypeConvertible[markedModel, string, markedConverter]
	value string
}

type markedConverter struct{}

func (markedConverter) ToTransfer(_ context.Context, m *markedModel) (*string, error) {
	return &m.value, nil
}
func (markedConverter) FromTransfer(_ context.Context, s *string, m *markedModel) error {
	m.value = *s
	return nil
}

func (markedConverter) ToTransferWithWorkflowContext(_ Context, m *markedModel) (*string, error) {
	return &m.value, nil
}

func (markedConverter) FromTransferWithWorkflowContext(_ Context, s *string, m *markedModel) error {
	m.value = *s
	return nil
}

type emptyConverter[M any] struct{}

func (emptyConverter[M]) ToTransfer(context.Context, *M) (*string, error) {
	s := "transfer"
	return &s, nil
}
func (emptyConverter[M]) FromTransfer(context.Context, *string, *M) error { return nil }

func (emptyConverter[M]) ToTransferWithWorkflowContext(Context, *M) (*string, error) {
	s := "transfer"
	return &s, nil
}

func (emptyConverter[M]) FromTransferWithWorkflowContext(Context, *string, *M) error { return nil }

type inheritedMarker struct {
	markedModel
}

type wrongMarker struct {
	TransferTypeConvertible[markedModel, string, markedConverter]
}

type pointerModelMarker struct {
	TransferTypeConvertible[*markedModel, string, emptyConverter[*markedModel]]
}
type interfaceModelMarker struct {
	TransferTypeConvertible[any, string, emptyConverter[any]]
}
type pointerConverterMarker struct {
	TransferTypeConvertible[pointerConverterMarker, string, *emptyConverter[pointerConverterMarker]]
}
type interfaceConverterMarker struct {
	TransferTypeConvertible[interfaceConverterMarker, string, TransferTypeConverter[interfaceConverterMarker, string]]
}
type functionConverter func()

func (functionConverter) ToTransfer(context.Context, *functionConverterMarker) (*string, error) {
	panic("invalid converter called")
}
func (functionConverter) FromTransfer(context.Context, *string, *functionConverterMarker) error {
	panic("invalid converter called")
}

func (functionConverter) ToTransferWithWorkflowContext(Context, *functionConverterMarker) (*string, error) {
	panic("invalid converter called")
}

func (functionConverter) FromTransferWithWorkflowContext(Context, *string, *functionConverterMarker) error {
	panic("invalid converter called")
}

type functionConverterMarker struct {
	TransferTypeConvertible[functionConverterMarker, string, functionConverter]
}

type firstMarker = TransferTypeConvertible[multipleMarkers, string, emptyConverter[multipleMarkers]]
type secondMarker = TransferTypeConvertible[multipleMarkers, int, integerConverter]
type multipleMarkers struct {
	firstMarker
	secondMarker
}
type integerConverter struct{}

func (integerConverter) ToTransfer(context.Context, *multipleMarkers) (*int, error) {
	return new(int), nil
}
func (integerConverter) FromTransfer(context.Context, *int, *multipleMarkers) error { return nil }

func (integerConverter) ToTransferWithWorkflowContext(Context, *multipleMarkers) (*int, error) {
	return new(int), nil
}

func (integerConverter) FromTransferWithWorkflowContext(Context, *int, *multipleMarkers) error {
	return nil
}

func TestTransferTypeConvertibleInvalidAssociations(t *testing.T) {
	for _, test := range []struct {
		value   any
		message string
	}{
		{wrongMarker{}, "must match enclosing struct"},
		{pointerModelMarker{}, "must match enclosing struct"},
		{interfaceModelMarker{}, "must match enclosing struct"},
		{pointerConverterMarker{}, "concrete nonpointer"},
		{interfaceConverterMarker{}, "concrete nonpointer"},
		{functionConverterMarker{}, "concrete nonpointer"},
		{inheritedMarker{}, "must match enclosing struct"},
	} {
		t.Run(fmt.Sprintf("%T", test.value), func(t *testing.T) {
			dc := makeTransferAware(nil)
			payload, err := converter.GetDefaultDataConverter().ToPayload("transfer")
			require.NoError(t, err)
			assertPanic := func(f func()) {
				t.Helper()
				defer func() {
					value := recover()
					require.IsType(t, "", value)
					require.Contains(t, value, "transfer type converter:")
					require.Contains(t, value, test.message)
				}()
				f()
			}
			assertPanic(func() { _, _ = dc.ToPayload(test.value) })
			destination := reflect.New(reflect.TypeOf(test.value))
			assertPanic(func() { _ = dc.FromPayload(payload, destination.Interface()) })
		})
	}
}

func TestTransferTypeConvertibleOrdinarySerialization(t *testing.T) {
	type namedField struct {
		Marker TransferTypeConvertible[markedModel, string, markedConverter]
		Label  string
	}
	for _, input := range []any{multipleMarkers{firstMarker: firstMarker{}, secondMarker: secondMarker{}}, namedField{Label: "outer"}} {
		dc := makeTransferAware(nil)
		payload, err := dc.ToPayload(input)
		require.NoError(t, err)
		want, err := converter.GetDefaultDataConverter().ToPayload(input)
		require.NoError(t, err)
		require.Equal(t, want, payload)
		got := reflect.New(reflect.TypeOf(input)).Interface()
		require.NoError(t, dc.FromPayload(payload, got))
		require.Equal(t, input, reflect.ValueOf(got).Elem().Interface())
	}
}

var errMarkedConversion = errors.New("marked conversion failed")

type failingModel struct {
	TransferTypeConvertible[failingModel, string, failingConverter]
}
type failingConverter struct{}

func (failingConverter) ToTransfer(context.Context, *failingModel) (*string, error) {
	return nil, errMarkedConversion
}
func (failingConverter) FromTransfer(context.Context, *string, *failingModel) error {
	return errMarkedConversion
}

func (failingConverter) ToTransferWithWorkflowContext(Context, *failingModel) (*string, error) {
	return nil, errMarkedConversion
}

func (failingConverter) FromTransferWithWorkflowContext(Context, *string, *failingModel) error {
	return errMarkedConversion
}

func TestTransferTypeConvertibleContextDispatchAndErrors(t *testing.T) {
	base := makeTransferAware(nil)
	for _, dc := range []converter.DataConverter{base, base.WithContext(t.Context()), base.WithWorkflowContext(Background())} {
		payload, err := dc.ToPayload(markedModel{value: "context-independent"})
		require.NoError(t, err)
		var got markedModel
		require.NoError(t, dc.FromPayload(payload, &got))
		require.Equal(t, "context-independent", got.value)
		_, err = dc.ToPayload(failingModel{})
		require.Same(t, errMarkedConversion, err)
		_, err = dc.ToPayloads(failingModel{})
		require.ErrorIs(t, err, errMarkedConversion)
		require.Same(t, errMarkedConversion, dc.FromPayload(payload, &failingModel{}))
		require.ErrorIs(t, dc.FromPayloads(&commonpb.Payloads{Payloads: []*commonpb.Payload{payload}}, &failingModel{}), errMarkedConversion)
	}
}
