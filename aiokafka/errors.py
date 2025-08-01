from collections.abc import Iterable
from typing import Any, TypeVar

__all__ = [
    # aiokafka custom errors
    "ConsumerStoppedError",
    "NoOffsetForPartitionError",
    "RecordTooLargeError",
    "ProducerClosed",
    # Kafka Python errors
    "KafkaError",
    "IllegalStateError",
    "IllegalArgumentError",
    "NoBrokersAvailable",
    "NodeNotReadyError",
    "KafkaProtocolError",
    "CorrelationIdError",
    "Cancelled",
    "TooManyInFlightRequests",
    "StaleMetadata",
    "UnrecognizedBrokerVersion",
    "IncompatibleBrokerVersion",
    "CommitFailedError",
    "AuthenticationMethodNotSupported",
    "AuthenticationFailedError",
    "BrokerResponseError",
    # Numbered errors
    "NoError",  # 0
    "UnknownError",  # -1
    "OffsetOutOfRangeError",  # 1
    "CorruptRecordException",  # 2
    "UnknownTopicOrPartitionError",  # 3
    "InvalidFetchRequestError",  # 4
    "LeaderNotAvailableError",  # 5
    "NotLeaderForPartitionError",  # 6
    "RequestTimedOutError",  # 7
    "BrokerNotAvailableError",  # 8
    "ReplicaNotAvailableError",  # 9
    "MessageSizeTooLargeError",  # 10
    "StaleControllerEpochError",  # 11
    "OffsetMetadataTooLargeError",  # 12
    "StaleLeaderEpochCodeError",  # 13
    "GroupLoadInProgressError",  # 14
    "GroupCoordinatorNotAvailableError",  # 15
    "NotCoordinatorForGroupError",  # 16
    "InvalidTopicError",  # 17
    "RecordListTooLargeError",  # 18
    "NotEnoughReplicasError",  # 19
    "NotEnoughReplicasAfterAppendError",  # 20
    "InvalidRequiredAcksError",  # 21
    "IllegalGenerationError",  # 22
    "InconsistentGroupProtocolError",  # 23
    "InvalidGroupIdError",  # 24
    "UnknownMemberIdError",  # 25
    "InvalidSessionTimeoutError",  # 26
    "RebalanceInProgressError",  # 27
    "InvalidCommitOffsetSizeError",  # 28
    "TopicAuthorizationFailedError",  # 29
    "GroupAuthorizationFailedError",  # 30
    "ClusterAuthorizationFailedError",  # 31
    "InvalidTimestampError",  # 32
    "UnsupportedSaslMechanismError",  # 33
    "IllegalSaslStateError",  # 34
    "UnsupportedVersionError",  # 35
    "TopicAlreadyExistsError",  # 36
    "InvalidPartitionsError",  # 37
    "InvalidReplicationFactorError",  # 38
    "InvalidReplicationAssignmentError",  # 39
    "InvalidConfigurationError",  # 40
    "NotControllerError",  # 41
    "InvalidRequestError",  # 42
    "UnsupportedForMessageFormatError",  # 43
    "PolicyViolationError",  # 44
    "KafkaUnavailableError",
    "KafkaTimeoutError",
    "KafkaConnectionError",
    "UnsupportedCodecError",
]


class KafkaError(RuntimeError):
    retriable = False
    # whether metadata should be refreshed on error
    invalid_metadata = False

    def __str__(self) -> str:
        if not self.args:
            return self.__class__.__name__
        return f"{self.__class__.__name__}: {super().__str__()}"


class IllegalStateError(KafkaError):
    pass


class IllegalArgumentError(KafkaError):
    pass


class NoBrokersAvailable(KafkaError):
    retriable = True
    invalid_metadata = True


class NodeNotReadyError(KafkaError):
    retriable = True


class KafkaProtocolError(KafkaError):
    retriable = True


class CorrelationIdError(KafkaProtocolError):
    retriable = True


class Cancelled(KafkaError):
    retriable = True


class TooManyInFlightRequests(KafkaError):
    retriable = True


class StaleMetadata(KafkaError):
    retriable = True
    invalid_metadata = True


class MetadataEmptyBrokerList(KafkaError):
    retriable = True


class UnrecognizedBrokerVersion(KafkaError):
    pass


class IncompatibleBrokerVersion(KafkaError):
    pass


class CommitFailedError(KafkaError):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(
            """Commit cannot be completed since the group has already
            rebalanced and assigned the partitions to another member.
            This means that the time between subsequent calls to poll()
            was longer than the configured max_poll_interval_ms, which
            typically implies that the poll loop is spending too much
            time message processing. You can address this either by
            increasing the rebalance timeout with max_poll_interval_ms,
            or by reducing the maximum size of batches returned in poll()
            with max_poll_records.
            """,
            *args,
            **kwargs,
        )


class AuthenticationMethodNotSupported(KafkaError):
    pass


class AuthenticationFailedError(KafkaError):
    retriable = False


class KafkaUnavailableError(KafkaError):
    pass


class KafkaTimeoutError(KafkaError):
    pass


class KafkaConnectionError(KafkaError):
    retriable = True
    invalid_metadata = True


class UnsupportedCodecError(KafkaError):
    pass


class KafkaConfigurationError(KafkaError):
    pass


class QuotaViolationError(KafkaError):
    pass


class ConsumerStoppedError(Exception):
    """Raised on `get*` methods of Consumer if it's cancelled, even pending
    ones.
    """


class IllegalOperation(Exception):
    """Raised if you try to execute an operation, that is not available with
    current configuration. For example trying to commit if no group_id was
    given.
    """


class NoOffsetForPartitionError(KafkaError):
    pass


class RecordTooLargeError(KafkaError):
    pass


class ProducerClosed(KafkaError):
    pass


class ProducerFenced(KafkaError):
    """Another producer with the same transactional ID went online.
    NOTE: As it seems this will be raised by Broker if transaction timeout
    occurred also.
    """

    def __init__(
        self,
        msg: str = (
            "There is a newer producer using the same transactional_id or"
            "transaction timeout occurred (check that processing time is "
            "below transaction_timeout_ms)"
        ),
    ) -> None:
        super().__init__(msg)


class BrokerResponseError(KafkaError):
    errno: int
    message: str
    description: str = ""

    def __str__(self) -> str:
        """Add errno to standard KafkaError str"""
        return f"[Error {self.errno}] {super().__str__()}"


class NoError(BrokerResponseError):
    errno = 0
    message = "NO_ERROR"
    description = "No error--it worked!"


class UnknownError(BrokerResponseError):
    errno = -1
    message = "UNKNOWN"
    description = "An unexpected server error."


class OffsetOutOfRangeError(BrokerResponseError):
    errno = 1
    message = "OFFSET_OUT_OF_RANGE"
    description = (
        "The requested offset is outside the range of offsets"
        " maintained by the server for the given topic/partition."
    )


class CorruptRecordException(BrokerResponseError):
    errno = 2
    message = "CORRUPT_MESSAGE"
    description = (
        "This message has failed its CRC checksum, exceeds the"
        " valid size, or is otherwise corrupt."
    )


# Backward compatibility
InvalidMessageError = CorruptRecordException


class UnknownTopicOrPartitionError(BrokerResponseError):
    errno = 3
    message = "UNKNOWN_TOPIC_OR_PARTITION"
    description = (
        "This request is for a topic or partition that does not"
        " exist on this broker."
    )
    retriable = True
    invalid_metadata = True


class InvalidFetchRequestError(BrokerResponseError):
    errno = 4
    message = "INVALID_FETCH_SIZE"
    description = "The message has a negative size."


class LeaderNotAvailableError(BrokerResponseError):
    errno = 5
    message = "LEADER_NOT_AVAILABLE"
    description = (
        "This error is thrown if we are in the middle of a"
        " leadership election and there is currently no leader for"
        " this partition and hence it is unavailable for writes."
    )
    retriable = True
    invalid_metadata = True


class NotLeaderForPartitionError(BrokerResponseError):
    errno = 6
    message = "NOT_LEADER_FOR_PARTITION"
    description = (
        "This error is thrown if the client attempts to send"
        " messages to a replica that is not the leader for some"
        " partition. It indicates that the clients metadata is out"
        " of date."
    )
    retriable = True
    invalid_metadata = True


class RequestTimedOutError(BrokerResponseError):
    errno = 7
    message = "REQUEST_TIMED_OUT"
    description = (
        "This error is thrown if the request exceeds the"
        " user-specified time limit in the request."
    )
    retriable = True


class BrokerNotAvailableError(BrokerResponseError):
    errno = 8
    message = "BROKER_NOT_AVAILABLE"
    description = (
        "This is not a client facing error and is used mostly by"
        " tools when a broker is not alive."
    )


class ReplicaNotAvailableError(BrokerResponseError):
    errno = 9
    message = "REPLICA_NOT_AVAILABLE"
    description = (
        "If replica is expected on a broker, but is not (this can be"
        " safely ignored)."
    )


class MessageSizeTooLargeError(BrokerResponseError):
    errno = 10
    message = "MESSAGE_SIZE_TOO_LARGE"
    description = (
        "The server has a configurable maximum message size to avoid"
        " unbounded memory allocation. This error is thrown if the"
        " client attempt to produce a message larger than this"
        " maximum."
    )


class StaleControllerEpochError(BrokerResponseError):
    errno = 11
    message = "STALE_CONTROLLER_EPOCH"
    description = "Internal error code for broker-to-broker communication."


class OffsetMetadataTooLargeError(BrokerResponseError):
    errno = 12
    message = "OFFSET_METADATA_TOO_LARGE"
    description = (
        "If you specify a string larger than configured maximum for offset metadata."
    )


# TODO is this deprecated?
# https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ErrorCodes
class StaleLeaderEpochCodeError(BrokerResponseError):
    errno = 13
    message = "STALE_LEADER_EPOCH_CODE"


class GroupLoadInProgressError(BrokerResponseError):
    errno = 14
    message = "COORDINATOR_LOAD_IN_PROGRESS"
    description = (
        "The broker returns this error code for an offset fetch"
        " request if it is still loading offsets (after a leader"
        " change for that offsets topic partition), or in response"
        " to group membership requests (such as heartbeats) when"
        " group metadata is being loaded by the coordinator."
    )
    retriable = True


CoordinatorLoadInProgressError = GroupLoadInProgressError


class GroupCoordinatorNotAvailableError(BrokerResponseError):
    errno = 15
    message = "COORDINATOR_NOT_AVAILABLE"
    description = (
        "The broker returns this error code for group coordinator"
        " requests, offset commits, and most group management"
        " requests if the offsets topic has not yet been created, or"
        " if the group coordinator is not active."
    )
    retriable = True


CoordinatorNotAvailableError = GroupCoordinatorNotAvailableError


class NotCoordinatorForGroupError(BrokerResponseError):
    errno = 16
    message = "NOT_COORDINATOR"
    description = (
        "The broker returns this error code if it receives an offset"
        " fetch or commit request for a group that it is not a"
        " coordinator for."
    )
    retriable = True


NotCoordinatorError = NotCoordinatorForGroupError


class InvalidTopicError(BrokerResponseError):
    errno = 17
    message = "INVALID_TOPIC"
    description = (
        "For a request which attempts to access an invalid topic"
        " (e.g. one which has an illegal name), or if an attempt"
        " is made to write to an internal topic (such as the"
        " consumer offsets topic)."
    )


class RecordListTooLargeError(BrokerResponseError):
    errno = 18
    message = "RECORD_LIST_TOO_LARGE"
    description = (
        "If a message batch in a produce request exceeds the maximum"
        " configured segment size."
    )


class NotEnoughReplicasError(BrokerResponseError):
    errno = 19
    message = "NOT_ENOUGH_REPLICAS"
    description = (
        "Returned from a produce request when the number of in-sync"
        " replicas is lower than the configured minimum and"
        " requiredAcks is -1."
    )
    retriable = True


class NotEnoughReplicasAfterAppendError(BrokerResponseError):
    errno = 20
    message = "NOT_ENOUGH_REPLICAS_AFTER_APPEND"
    description = (
        "Returned from a produce request when the message was"
        " written to the log, but with fewer in-sync replicas than"
        " required."
    )
    retriable = True


class InvalidRequiredAcksError(BrokerResponseError):
    errno = 21
    message = "INVALID_REQUIRED_ACKS"
    description = (
        "Returned from a produce request if the requested"
        " requiredAcks is invalid (anything other than -1, 1, or 0)."
    )


class IllegalGenerationError(BrokerResponseError):
    errno = 22
    message = "ILLEGAL_GENERATION"
    description = (
        "Returned from group membership requests (such as heartbeats)"
        " when the generation id provided in the request is not the"
        " current generation."
    )


class InconsistentGroupProtocolError(BrokerResponseError):
    errno = 23
    message = "INCONSISTENT_GROUP_PROTOCOL"
    description = (
        "Returned in join group when the member provides a protocol"
        " type or set of protocols which is not compatible with the"
        " current group."
    )


class InvalidGroupIdError(BrokerResponseError):
    errno = 24
    message = "INVALID_GROUP_ID"
    description = "Returned in join group when the groupId is empty or null."


class UnknownMemberIdError(BrokerResponseError):
    errno = 25
    message = "UNKNOWN_MEMBER_ID"
    description = (
        "Returned from group requests (offset commits/fetches,"
        " heartbeats, etc) when the memberId is not in the current"
        " generation."
    )


class InvalidSessionTimeoutError(BrokerResponseError):
    errno = 26
    message = "INVALID_SESSION_TIMEOUT"
    description = (
        "Return in join group when the requested session timeout is"
        " outside of the allowed range on the broker"
    )


class RebalanceInProgressError(BrokerResponseError):
    errno = 27
    message = "REBALANCE_IN_PROGRESS"
    description = (
        "Returned in heartbeat requests when the coordinator has"
        " begun rebalancing the group. This indicates to the client"
        " that it should rejoin the group."
    )


class InvalidCommitOffsetSizeError(BrokerResponseError):
    errno = 28
    message = "INVALID_COMMIT_OFFSET_SIZE"
    description = (
        "This error indicates that an offset commit was rejected"
        " because of oversize metadata."
    )


class TopicAuthorizationFailedError(BrokerResponseError):
    errno = 29
    message = "TOPIC_AUTHORIZATION_FAILED"
    description = (
        "Returned by the broker when the client is not authorized to"
        " access the requested topic."
    )


class GroupAuthorizationFailedError(BrokerResponseError):
    errno = 30
    message = "GROUP_AUTHORIZATION_FAILED"
    description = (
        "Returned by the broker when the client is not authorized to"
        " access a particular groupId."
    )


class ClusterAuthorizationFailedError(BrokerResponseError):
    errno = 31
    message = "CLUSTER_AUTHORIZATION_FAILED"
    description = (
        "Returned by the broker when the client is not authorized to"
        " use an inter-broker or administrative API."
    )


class InvalidTimestampError(BrokerResponseError):
    errno = 32
    message = "INVALID_TIMESTAMP"
    description = "The timestamp of the message is out of acceptable range."


class UnsupportedSaslMechanismError(BrokerResponseError):
    errno = 33
    message = "UNSUPPORTED_SASL_MECHANISM"
    description = "The broker does not support the requested SASL mechanism."


class IllegalSaslStateError(BrokerResponseError):
    errno = 34
    message = "ILLEGAL_SASL_STATE"
    description = "Request is not valid given the current SASL state."


class UnsupportedVersionError(BrokerResponseError):
    errno = 35
    message = "UNSUPPORTED_VERSION"
    description = "The version of API is not supported."


class TopicAlreadyExistsError(BrokerResponseError):
    errno = 36
    message = "TOPIC_ALREADY_EXISTS"
    description = "Topic with this name already exists."


class InvalidPartitionsError(BrokerResponseError):
    errno = 37
    message = "INVALID_PARTITIONS"
    description = "Number of partitions is invalid."


class InvalidReplicationFactorError(BrokerResponseError):
    errno = 38
    message = "INVALID_REPLICATION_FACTOR"
    description = "Replication-factor is invalid."


class InvalidReplicationAssignmentError(BrokerResponseError):
    errno = 39
    message = "INVALID_REPLICATION_ASSIGNMENT"
    description = "Replication assignment is invalid."


class InvalidConfigurationError(BrokerResponseError):
    errno = 40
    message = "INVALID_CONFIG"
    description = "Configuration is invalid."


class NotControllerError(BrokerResponseError):
    errno = 41
    message = "NOT_CONTROLLER"
    description = "This is not the correct controller for this cluster."
    retriable = True


class InvalidRequestError(BrokerResponseError):
    errno = 42
    message = "INVALID_REQUEST"
    description = (
        "This most likely occurs because of a request being"
        " malformed by the client library or the message was"
        " sent to an incompatible broker. See the broker logs"
        " for more details."
    )


class UnsupportedForMessageFormatError(BrokerResponseError):
    errno = 43
    message = "UNSUPPORTED_FOR_MESSAGE_FORMAT"
    description = (
        "The message format version on the broker does not support this request."
    )


class PolicyViolationError(BrokerResponseError):
    errno = 44
    message = "POLICY_VIOLATION"
    description = "Request parameters do not satisfy the configured policy."


class OutOfOrderSequenceNumber(BrokerResponseError):
    errno = 45
    message = "OUT_OF_ORDER_SEQUENCE_NUMBER"
    description = "The broker received an out of order sequence number"


class DuplicateSequenceNumber(BrokerResponseError):
    errno = 46
    message = "DUPLICATE_SEQUENCE_NUMBER"
    description = "The broker received a duplicate sequence number"


class InvalidProducerEpoch(BrokerResponseError):
    errno = 47
    message = "INVALID_PRODUCER_EPOCH"
    description = (
        "Producer attempted an operation with an old epoch. Either "
        "there is a newer producer with the same transactionalId, or the "
        "producer's transaction has been expired by the broker."
    )


class InvalidTxnState(BrokerResponseError):
    errno = 48
    message = "INVALID_TXN_STATE"
    description = "The producer attempted a transactional operation in an invalid state"


class InvalidProducerIdMapping(BrokerResponseError):
    errno = 49
    message = "INVALID_PRODUCER_ID_MAPPING"
    description = (
        "The producer attempted to use a producer id which is not currently "
        "assigned to its transactional id"
    )


class InvalidTransactionTimeout(BrokerResponseError):
    errno = 50
    message = "INVALID_TRANSACTION_TIMEOUT"
    description = (
        "The transaction timeout is larger than the maximum value allowed by"
        " the broker (as configured by transaction.max.timeout.ms)."
    )


class ConcurrentTransactions(BrokerResponseError):
    errno = 51
    message = "CONCURRENT_TRANSACTIONS"
    description = (
        "The producer attempted to update a transaction while another "
        "concurrent operation on the same transaction was ongoing"
    )


class TransactionCoordinatorFenced(BrokerResponseError):
    errno = 52
    message = "TRANSACTION_COORDINATOR_FENCED"
    description = (
        "Indicates that the transaction coordinator sending a WriteTxnMarker"
        " is no longer the current coordinator for a given producer"
    )


class TransactionalIdAuthorizationFailed(BrokerResponseError):
    errno = 53
    message = "TRANSACTIONAL_ID_AUTHORIZATION_FAILED"
    description = "Transactional Id authorization failed"


class SecurityDisabled(BrokerResponseError):
    errno = 54
    message = "SECURITY_DISABLED"
    description = "Security features are disabled"


class OperationNotAttempted(BrokerResponseError):
    errno = 55
    message = "OPERATION_NOT_ATTEMPTED"
    description = (
        "The broker did not attempt to execute this operation. This may happen"
        " for batched RPCs where some operations in the batch failed, causing "
        "the broker to respond without trying the rest."
    )


class KafkaStorageError(BrokerResponseError):
    errno = 56
    message = "KAFKA_STORAGE_ERROR"
    description = "Disk error when trying to access log file on the disk."
    retriable = True
    invalid_metadata = True


class LogDirNotFound(BrokerResponseError):
    errno = 57
    message = "LOG_DIR_NOT_FOUND"
    description = "The user-specified log directory is not found in the broker config."


class SaslAuthenticationFailed(BrokerResponseError):
    errno = 58
    message = "SASL_AUTHENTICATION_FAILED"
    description = "SASL Authentication failed."


class UnknownProducerId(BrokerResponseError):
    errno = 59
    message = "UNKNOWN_PRODUCER_ID"
    description = (
        "This exception is raised by the broker if it could not locate the "
        "producer metadata associated with the producerId in question. This "
        "could happen if, for instance, the producer's records were deleted "
        "because their retention time had elapsed. Once the last records of "
        "the producerId are removed, the producer's metadata is removed from"
        " the broker, and future appends by the producer will return this "
        "exception."
    )


class ReassignmentInProgress(BrokerResponseError):
    errno = 60
    message = "REASSIGNMENT_IN_PROGRESS"
    description = "A partition reassignment is in progress"


class DelegationTokenAuthDisabled(BrokerResponseError):
    errno = 61
    message = "DELEGATION_TOKEN_AUTH_DISABLED"
    description = "Delegation Token feature is not enabled"


class DelegationTokenNotFound(BrokerResponseError):
    errno = 62
    message = "DELEGATION_TOKEN_NOT_FOUND"
    description = "Delegation Token is not found on server."


class DelegationTokenOwnerMismatch(BrokerResponseError):
    errno = 63
    message = "DELEGATION_TOKEN_OWNER_MISMATCH"
    description = "Specified Principal is not valid Owner/Renewer."


class DelegationTokenRequestNotAllowed(BrokerResponseError):
    errno = 64
    message = "DELEGATION_TOKEN_REQUEST_NOT_ALLOWED"
    description = (
        "Delegation Token requests are not allowed on PLAINTEXT/1-way SSL "
        "channels and on delegation token authenticated channels."
    )


class DelegationTokenAuthorizationFailed(BrokerResponseError):
    errno = 65
    message = "DELEGATION_TOKEN_AUTHORIZATION_FAILED"
    description = "Delegation Token authorization failed."


class DelegationTokenExpired(BrokerResponseError):
    errno = 66
    message = "DELEGATION_TOKEN_EXPIRED"
    description = "Delegation Token is expired."


class InvalidPrincipalType(BrokerResponseError):
    errno = 67
    message = "INVALID_PRINCIPAL_TYPE"
    description = "Supplied principalType is not supported"


class NonEmptyGroup(BrokerResponseError):
    errno = 68
    message = "NON_EMPTY_GROUP"
    description = "The group is not empty"


class GroupIdNotFound(BrokerResponseError):
    errno = 69
    message = "GROUP_ID_NOT_FOUND"
    description = "The group id does not exist"


class FetchSessionIdNotFound(BrokerResponseError):
    errno = 70
    message = "FETCH_SESSION_ID_NOT_FOUND"
    description = "The fetch session ID was not found"


class InvalidFetchSessionEpoch(BrokerResponseError):
    errno = 71
    message = "INVALID_FETCH_SESSION_EPOCH"
    description = "The fetch session epoch is invalid"


class ListenerNotFound(BrokerResponseError):
    errno = 72
    message = "LISTENER_NOT_FOUND"
    description = (
        "There is no listener on the leader broker that matches the"
        " listener on which metadata request was processed"
    )


class MemberIdRequired(BrokerResponseError):
    errno = 79
    message = "MEMBER_ID_REQUIRED"
    description = (
        "Consumer needs to have a valid member id before actually entering group"
    )


_T = TypeVar("_T", bound=type)


def _iter_subclasses(cls: _T) -> Iterable[_T]:
    for subclass in cls.__subclasses__():
        yield subclass
        yield from _iter_subclasses(subclass)


kafka_errors = {x.errno: x for x in _iter_subclasses(BrokerResponseError)}


def for_code(error_code: int) -> type[BrokerResponseError]:
    return kafka_errors.get(error_code, UnknownError)
