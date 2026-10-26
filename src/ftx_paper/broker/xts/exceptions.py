"""Typed failures raised by the XTS market-data boundary."""


class XTSError(RuntimeError):
    """Base class for XTS provider failures."""


class XTSAuthenticationError(XTSError):
    """The market-data session is invalid or could not be refreshed."""


class XTSTransportError(XTSError):
    """The provider request could not be completed."""


class XTSApiError(XTSError):
    """XTS returned a structured API error."""
