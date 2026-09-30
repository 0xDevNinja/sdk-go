package internal

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
)

// -- USER API -----------------------------------------------------------

// TransferTypeConvertible is a marker struct that associates Model with a
// [TransferTypeConverter].
//
// The first time the SDK sees a Model value with an embedded
// TransferTypeConvertible[Model, Transfer, C], the SDK creates a zero value
// of C and caches it. Model values will be encoded by first applying
// C.ToTransfer and then applying the data converter to the resulting
// Transfer value. To decode a Model value, the SDK gets a Transfer value
// from the data converter and applies C.FromTransfer to it.
//
// The SDK will panic during data conversion if it sees a value of type T that
// embeds TransferTypeConvertible[A,B,C] where A != T. Do not embed a struct
// that embeds TransferTypeConvertible inside another struct.
//
// NOTE: Experimental.
//
// Exposed as: [go.temporal.io/sdk/workflow.TransferTypeConvertible]
type TransferTypeConvertible[Model, Transfer any, C TransferTypeConverter[Model, Transfer]] struct{}

// TransferTypeConverter converts between a model type and its transfer type;
// see [TransferTypeConvertible] for usage.
// Implementations must be pure, threadsafe, and deterministic.
//
// Outside a workflow, data conversion will invoke ToTransfer/FromTransfer with
// the context provided by [ContextAware.WithContext] if one is available;
// otherwise it will use context.Background().
//
// Inside a workflow, data conversion will instead pass the workflow context
// provided by [ContextAware.WithWorkflowContext].
//
// Exposed as: [go.temporal.io/sdk/workflow.TransferTypeConverter]
type TransferTypeConverter[Model, Transfer any] interface {
	ToTransfer(context.Context, *Model) (*Transfer, error)
	FromTransfer(context.Context, *Transfer, *Model) error
	ToTransferWithWorkflowContext(Context, *Model) (*Transfer, error)
	FromTransferWithWorkflowContext(Context, *Transfer, *Model) error
}


// -- INTERNAL API -----------------------------------------------------------

// transferTypeConvertible is a handle to a [TransferTypeConvertible] that
// you can use without knowing its generic type args.
type transferTypeConvertible interface {
	transferTypeConverter(reflect.Type) transferTypeConverter
}

func (*TransferTypeConvertible[Model, Transfer, C]) transferTypeConverter(model reflect.Type) transferTypeConverter {
	modelType := reflect.TypeFor[Model]()
	if modelType.Kind() != reflect.Struct || modelType != model {
		panic(fmt.Sprintf("transfer type converter: marker model %v must match enclosing struct %v", modelType, model))
	}
	converterType := reflect.TypeFor[C]()
	switch converterType.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Func, reflect.Map, reflect.Slice, reflect.Chan, reflect.UnsafePointer:
		panic(fmt.Sprintf("transfer type converter: converter %v must be a concrete nonpointer type with a usable zero value", converterType))
	}
	var c C
	return &transferTypeConverterImpl[Model, Transfer]{converter: c}
}

// transferTypeConverter is a handle to a [TransferTypeConverter] that
// you can use without knowing its generic type args.
type transferTypeConverter interface {
	// newTransferTypePtr returns a pointer to a zero transfer value.
	newTransferTypePtr() any

	toTransferTypeWithContext(ctx context.Context, value any) (any, error)
	fromTransferTypeWithContext(ctx context.Context, transferTypePtr any, valuePtr any) error

	toTransferTypeWithWorkflowContext(ctx Context, value any) (any, error)
	fromTransferTypeWithWorkflowContext(ctx Context, transferTypePtr any, valuePtr any) error
}


type transferTypeConverterImpl[ModelType, TransferType any] struct {
	converter TransferTypeConverter[ModelType, TransferType]
}

func (*transferTypeConverterImpl[ModelType, TransferType]) newTransferTypePtr() any {
	return new(TransferType)
}

func (tc *transferTypeConverterImpl[ModelType, TransferType]) toTransferTypeWithContext(ctx context.Context, value any) (any, error) {
	switch v := value.(type) {
	case *ModelType:
		return tc.converter.ToTransfer(ctx, v)
	case ModelType:
		return tc.converter.ToTransfer(ctx, &v)
	default:
		var zero ModelType
		panic(fmt.Sprintf("transfer type converter: want value of type %T or %T, got %T", zero, (*ModelType)(nil), value))
	}
}

func (tc *transferTypeConverterImpl[ModelType, TransferType]) fromTransferTypeWithContext(ctx context.Context, transferTypePtr any, valuePtr any) error {
	v, ok := valuePtr.(*ModelType)
	if !ok {
		panic(fmt.Sprintf("transfer type converter: want value of type %T, got %T", (*ModelType)(nil), valuePtr))
	}
	tvp, ok := transferTypePtr.(*TransferType)
	if !ok {
		panic(fmt.Sprintf("transfer type converter: want transfer value of type %T, got %T", (*TransferType)(nil), transferTypePtr))
	}
	return tc.converter.FromTransfer(ctx, tvp, v)
}

func (tc *transferTypeConverterImpl[ModelType, TransferType]) toTransferTypeWithWorkflowContext(ctx Context, value any) (any, error) {
	switch v := value.(type) {
	case *ModelType:
		return tc.converter.ToTransferWithWorkflowContext(ctx, v)
	case ModelType:
		return tc.converter.ToTransferWithWorkflowContext(ctx, &v)
	default:
		var zero ModelType
		panic(fmt.Sprintf("transfer type converter: want value of type %T or %T, got %T", zero, (*ModelType)(nil), value))
	}
}

func (tc *transferTypeConverterImpl[ModelType, TransferType]) fromTransferTypeWithWorkflowContext(ctx Context, transferTypePtr any, valuePtr any) error {
	v, ok := valuePtr.(*ModelType)
	if !ok {
		panic(fmt.Sprintf("transfer type converter: want value of type %T, got %T", (*ModelType)(nil), valuePtr))
	}
	tvp, ok := transferTypePtr.(*TransferType)
	if !ok {
		panic(fmt.Sprintf("transfer type converter: want transfer value of type %T, got %T", (*TransferType)(nil), transferTypePtr))
	}
	return tc.converter.FromTransferWithWorkflowContext(ctx, tvp, v)
}


// isTransferConvertible returns true if model is transfer-type convertible,
// i.e. it's a struct type that embeds [TransferTypeConvertible].
func isTransferConvertible(model reflect.Type) bool {
	return model != nil && model.Kind() == reflect.Struct &&
		reflect.PointerTo(model).Implements(reflect.TypeFor[transferTypeConvertible]())
}

// transferTypeConverterOrPanic returns a [transferTypeConverter] for model,
// panicking if model isn't transfer-type convertible.
func transferTypeConverterOrPanic(model reflect.Type) transferTypeConverter {

	// Never consult application-instance state, including initialized embedded pointers.
	marker := reflect.New(model).Interface().(transferTypeConvertible)
	return marker.transferTypeConverter(model)
}

// -- DATA CONVERTERS ----------------------------------------------------------

// The default data converter, wrapped so it supports transfer type conversion.
var defaultTransferAwareDataConverter = makeTransferAware(converter.GetDefaultDataConverter())

// transferAwareDataConverter is a context-aware data converter that:
//
//  1. Encodes its input by trying to apply transfer conversion and forwarding
//     the result to the parent data converter; and
//  2. Decodes its input using the parent data converter and then trying to
//     transfer-convert the result into a normal value.
type transferAwareDataConverter struct {
	parent                 converter.DataConverter
	context                context.Context
	workflowContext        Context
	transferTypeConverters *sync.Map
}

var _ converter.DataConverter = (*transferAwareDataConverter)(nil)
var _ converter.DataConverterWithSerializationContext = (*transferAwareDataConverter)(nil)
var _ ContextAware = (*transferAwareDataConverter)(nil)

// makeTransferAware is an idempotent operation that upgrades a
// normal data converter into a transfer-type-aware data converter.
// If dc is nil, it wraps the default data converter.
func makeTransferAware(dc converter.DataConverter) *transferAwareDataConverter {
	if dc == nil {
		dc = converter.GetDefaultDataConverter()
	}
	if tadc, ok := dc.(*transferAwareDataConverter); ok {
		return tadc
	}
	return &transferAwareDataConverter{
		parent:                 dc,
		transferTypeConverters: new(sync.Map),
	}
}

func (dc *transferAwareDataConverter) transferTypeConverter(value any) transferTypeConverter {
	valueType := reflect.TypeOf(value)
	if valueType == nil {
		return nil
	}
	if valueType.Kind() == reflect.Pointer {
		valueType = valueType.Elem()
	}
	if tc, ok := dc.transferTypeConverters.Load(valueType); ok {
		return tc.(transferTypeConverter)
	}
	if !isTransferConvertible(valueType) {
		return nil
	}
	tc := transferTypeConverterOrPanic(valueType)
	cached, _ := dc.transferTypeConverters.LoadOrStore(valueType, tc)
	return cached.(transferTypeConverter)
}

func (dc *transferAwareDataConverter) ToPayload(value any) (*commonpb.Payload, error) {
	transferType, err := dc.encodeAsTransferTypeOrReturn(value)
	if err != nil {
		return nil, err
	}
	return dc.parent.ToPayload(transferType)
}

func (dc *transferAwareDataConverter) ToPayloads(values ...any) (*commonpb.Payloads, error) {
	// TODO Would callers be surprised if we mutated their array? See encodeArgs for instance.
	// Is it worth getting fancy to avoid this allocation in the common case where none of
	// the values are transfer-convertible?
	transferTypes := make([]any, len(values))
	for i, value := range values {
		transferType, err := dc.encodeAsTransferTypeOrReturn(value)
		if err != nil {
			return nil, fmt.Errorf("values[%d]: %w", i, err)
		}
		transferTypes[i] = transferType
	}
	return dc.parent.ToPayloads(transferTypes...)
}

func (dc *transferAwareDataConverter) encodeAsTransferTypeOrReturn(value any) (transferType any, err error) {
	tc := dc.transferTypeConverter(value)
	if tc == nil {
		return value, nil
	}
	return dc.toTransferType(tc, value)
}

// toTransferType converts value with whichever flavor of conversion suits the context
// this data converter is running in.
func (dc *transferAwareDataConverter) toTransferType(tc transferTypeConverter, value any) (any, error) {
	if dc.workflowContext != nil {
		return tc.toTransferTypeWithWorkflowContext(dc.workflowContext, value)
	}
	if dc.context != nil {
		return tc.toTransferTypeWithContext(dc.context, value)
	}
	return tc.toTransferTypeWithContext(context.Background(), value)
}

// fromTransferType is the [transferAwareDataConverter.toTransferType] counterpart.
func (dc *transferAwareDataConverter) fromTransferType(tc transferTypeConverter, transferTypePtr any, valuePtr any) error {
	if dc.workflowContext != nil {
		return tc.fromTransferTypeWithWorkflowContext(dc.workflowContext, transferTypePtr, valuePtr)
	}
	if dc.context != nil {
		return tc.fromTransferTypeWithContext(dc.context, transferTypePtr, valuePtr)
	}
	return tc.fromTransferTypeWithContext(context.Background(), transferTypePtr, valuePtr)
}

// transferDecodeDestination adapts **T to *T when *T implements the marker interface.
// Preserve existing instances; metadata discovery uses a separate fresh value.
func transferDecodeDestination(valuePtr any) any {
	value := reflect.ValueOf(valuePtr)
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return valuePtr
	}
	destination := value.Elem()
	if destination.Kind() != reflect.Pointer {
		return valuePtr
	}
	if !isTransferConvertible(destination.Type().Elem()) {
		return valuePtr
	}
	if destination.IsNil() {
		destination.Set(reflect.New(destination.Type().Elem()))
	}
	return destination.Interface()
}

func (dc *transferAwareDataConverter) FromPayload(payload *commonpb.Payload, valuePtr any) error {
	if payload == nil {
		return dc.parent.FromPayload(payload, valuePtr)
	}
	valuePtr = transferDecodeDestination(valuePtr)
	tc := dc.transferTypeConverter(valuePtr)
	if tc == nil {
		return dc.parent.FromPayload(payload, valuePtr)
	}
	transferTypePtr := tc.newTransferTypePtr()
	err := dc.parent.FromPayload(payload, transferTypePtr)
	if err != nil {
		return err
	}
	return dc.fromTransferType(tc, transferTypePtr, valuePtr)
}

func (dc *transferAwareDataConverter) FromPayloads(payloads *commonpb.Payloads, valuePtrs ...any) error {
	if payloads == nil {
		return dc.parent.FromPayloads(payloads, valuePtrs...)
	}
	// TODO Is it worth getting fancy to avoid this allocation in the common case where
	// none of the values are transfer-convertible?
	transferTypePtrs := make([]any, len(valuePtrs))
	destinations := make([]struct {
		valuePtr  any
		converter transferTypeConverter
	}, len(valuePtrs))
	n := min(len(payloads.GetPayloads()), len(valuePtrs))
	for i := range n {
		valuePtr := transferDecodeDestination(valuePtrs[i])
		tc := dc.transferTypeConverter(valuePtr)
		if tc == nil {
			transferTypePtrs[i] = valuePtrs[i]
		} else {
			destinations[i].valuePtr = valuePtr
			destinations[i].converter = tc
			transferTypePtrs[i] = tc.newTransferTypePtr()
		}
	}

	if err := dc.parent.FromPayloads(payloads, transferTypePtrs...); err != nil {
		return err
	}

	for i := range n {
		destination := destinations[i]
		if destination.converter == nil {
			valuePtrs[i] = transferTypePtrs[i]
		} else {
			err := dc.fromTransferType(
				destination.converter, transferTypePtrs[i], destination.valuePtr)
			if err != nil {
				return fmt.Errorf("transfer type converter: payload item %d: %w", i, err)
			}
		}
	}
	return nil
}

func (dc *transferAwareDataConverter) ToString(input *commonpb.Payload) string {
	return dc.parent.ToString(input)
}

func (dc *transferAwareDataConverter) ToStrings(input *commonpb.Payloads) []string {
	return dc.parent.ToStrings(input)
}

// WithSerializationContext forwards the serialization context to the parent data
// converter. Transfer converters have no use for it.
func (dc *transferAwareDataConverter) WithSerializationContext(ctx converter.SerializationContext) converter.DataConverter {
	if _, ok := dc.parent.(converter.DataConverterWithSerializationContext); !ok {
		return dc
	}
	result := *dc
	result.parent = converter.WithDataConverterSerializationContext(dc.parent, ctx)
	return &result
}

func (dc *transferAwareDataConverter) WithWorkflowContext(ctx Context) converter.DataConverter {
	result := *dc
	result.parent = WithWorkflowContext(ctx, dc.parent)
	result.context = nil
	result.workflowContext = ctx
	return &result
}

func (dc *transferAwareDataConverter) WithContext(ctx context.Context) converter.DataConverter {
	result := *dc
	result.parent = WithContext(ctx, dc.parent)
	result.context = ctx
	result.workflowContext = nil
	return &result
}
