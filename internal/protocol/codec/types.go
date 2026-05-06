// Package codec encodes/decodes Kafka request and response *frames* — i.e.,
// the fixed-size headers and primitive types that wrap each API call. It
// deliberately does NOT contain a decoded RecordBatch type or any per-record
// helpers; per v3.3 plan constraint #22, RecordBatch payloads are byte-opaque
// throughout the broker. Tests that need a decoded representation import
// tests/testutil/recordbatch.
package codec

// ErrorCode is a typed Kafka error code (int16).
type ErrorCode int16

const (
	ErrNone                        ErrorCode = 0
	ErrUnknownServerError          ErrorCode = -1
	ErrOffsetOutOfRange            ErrorCode = 1
	ErrCorruptMessage              ErrorCode = 2
	ErrUnknownTopicOrPartition     ErrorCode = 3
	ErrLeaderNotAvailable          ErrorCode = 5
	ErrNotLeaderOrFollower         ErrorCode = 6
	ErrRequestTimedOut             ErrorCode = 7
	ErrNetworkException            ErrorCode = 13
	ErrCoordinatorNotAvailable     ErrorCode = 15
	ErrNotCoordinator              ErrorCode = 16
	ErrInvalidTopicException       ErrorCode = 17
	ErrMessageTooLarge             ErrorCode = 18
	ErrGroupLoadInProgress         ErrorCode = 14
	ErrIllegalGeneration           ErrorCode = 22
	ErrInconsistentGroupProtocol   ErrorCode = 23
	ErrUnknownMemberId             ErrorCode = 25
	ErrInvalidSessionTimeout       ErrorCode = 26
	ErrRebalanceInProgress         ErrorCode = 27
	ErrTopicAuthorizationFailed    ErrorCode = 29
	ErrGroupAuthorizationFailed    ErrorCode = 30
	ErrUnsupportedSaslMechanism    ErrorCode = 33
	ErrUnsupportedVersion          ErrorCode = 35
	ErrTopicAlreadyExists          ErrorCode = 36
	ErrInvalidPartitions           ErrorCode = 37
	ErrInvalidRequest              ErrorCode = 42
	ErrOutOfOrderSequenceNumber    ErrorCode = 45
	ErrDuplicateSequenceNumber     ErrorCode = 46
	ErrInvalidProducerEpoch        ErrorCode = 47
	ErrTransactionalIdAuthFailed   ErrorCode = 53
	ErrNonEmptyGroup               ErrorCode = 67
	ErrGroupIDNotFound             ErrorCode = 69
	ErrCoordinatorLoadInProgress   ErrorCode = 14
)

