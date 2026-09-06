"""Typed failures raised by the remoting protocol core."""


class RemotingError(Exception):
    """Base class for remoting failures."""


class NoAssociationError(RemotingError):
    """A remote operation requires an active association."""


class StaleIncarnationError(RemotingError):
    """A remote actor reference targets a superseded system incarnation."""


class RemotingCapacityError(RemotingError):
    """A bounded association resource cannot admit more work."""


class ResolutionError(RemotingError):
    """A remote actor path could not be resolved."""


class HandshakeError(RemotingError):
    """An association handshake failed or timed out."""


class RemotingShutdownError(RemotingError):
    """An operation cannot run because its remoting runtime is stopping."""


class ProtocolValidationError(RemotingError, ValueError):
    """A locally supplied protocol value is outside the v1 contract."""


class SerializerRegistryError(RemotingError, ValueError):
    """A serializer registry cannot be built from the supplied entries."""


class UnknownSerializerError(RemotingError, LookupError):
    """No explicit serializer registration or exact-type binding exists."""


class UnsupportedManifestError(RemotingError, LookupError):
    """A serializer cannot use a manifest in the requested direction."""


class SerializationError(RemotingError):
    """A registered serializer failed to encode a value."""


class DeserializationError(RemotingError):
    """A registered serializer failed to decode a payload."""


class SerializerNegotiationError(RemotingError):
    """Peer serializer descriptors conflict."""


class WireCodecError(RemotingError):
    """Base class for malformed or unsupported wire input."""


class InvalidPreambleError(WireCodecError):
    """A transport preamble is malformed or does not match expectations."""


class MalformedFrameError(WireCodecError):
    """A frame is truncated, has trailing data, or contains an invalid value."""


class FrameTooLargeError(WireCodecError):
    """A declared or encoded frame exceeds its applicable bound."""


class UnsupportedFrameError(WireCodecError):
    """A frame type is not reserved by remoting v1."""


class UnsupportedFeatureError(WireCodecError):
    """Mandatory flags or a header version are unsupported."""


class UnsupportedHeaderVersionError(UnsupportedFeatureError):
    """A frame uses an incompatible common-header version."""


class WrongStreamError(WireCodecError):
    """A frame appeared on a stream kind on which it is not valid."""
