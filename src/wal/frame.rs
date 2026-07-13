//! WAL record framing.
//!
//! Every record in the log is wrapped in a fixed 8-byte header followed by the
//! opaque payload the engine handed us:
//!
//! ```text
//! ┌──────────────┬──────────────┬───────────────────────┐
//! │ payload_len  │ crc32(pay.)  │ payload (payload_len)  │
//! │  u32 LE (4)  │  u32 LE (4)  │        bytes           │
//! └──────────────┴──────────────┴───────────────────────┘
//! ```
//!
//! The CRC32 covers **only the payload**, matching the torn-tail recovery rule
//! documented in `DESIGN_NOTES.md` and demonstrated by the toy store in
//! `tests/harness.rs`: recovery stops at the first frame that is short (the
//! header or payload runs past end-of-file) or whose payload fails its CRC.
//!
//! Framing is payload-agnostic on purpose — the WAL never interprets record
//! bytes, so the memtable/record encoding can evolve independently of the log
//! format.

/// Size of the little-endian `u32` payload-length field.
pub(crate) const LEN_SZ: usize = 4;
/// Size of the little-endian `u32` CRC field.
pub(crate) const CRC_SZ: usize = 4;
/// Total per-record framing overhead (length + CRC).
pub(crate) const HEADER_SZ: usize = LEN_SZ + CRC_SZ;

/// Encode `payload` into a complete CRC-framed record ready to append.
///
/// The returned buffer is `HEADER_SZ + payload.len()` bytes.
pub(crate) fn encode(payload: &[u8]) -> Vec<u8> {
    let crc = crc32fast::hash(payload);
    let mut frame = Vec::with_capacity(HEADER_SZ + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&crc.to_le_bytes());
    frame.extend_from_slice(payload);
    frame
}

/// The successful decode of one frame sitting at some offset in a buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DecodedFrame {
    /// The record payload, copied out of the source buffer.
    pub payload: Vec<u8>,
    /// Total on-disk size of this frame (`HEADER_SZ + payload.len()`), i.e. how
    /// far to advance to reach the next frame.
    pub total_len: usize,
}

/// Why a frame could not be decoded from `buf[offset..]`.
///
/// Both variants mean the same thing to recovery — the log is torn here, so
/// everything from this offset on is discarded — but they are distinguished so
/// the crash journal can report *how* the tail was damaged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FrameError {
    /// The header or the declared payload runs past the end of the buffer: a
    /// classic truncated (torn) write.
    Truncated,
    /// The frame is fully present but its payload does not match the stored
    /// CRC: the bytes were corrupted (e.g. a bit-flip in an unsynced tail).
    BadCrc,
}

/// Attempt to decode one frame from `buf` starting at `offset`.
///
/// Returns [`FrameError::Truncated`] if the header or payload extends past the
/// end of `buf`, and [`FrameError::BadCrc`] if the payload's CRC does not
/// verify. On success the payload is copied out and the frame's total length is
/// reported so the caller can advance.
pub(crate) fn decode(buf: &[u8], offset: usize) -> Result<DecodedFrame, FrameError> {
    let header_end = offset.checked_add(HEADER_SZ).ok_or(FrameError::Truncated)?;
    if header_end > buf.len() {
        return Err(FrameError::Truncated);
    }
    let payload_len =
        u32::from_le_bytes(buf[offset..offset + LEN_SZ].try_into().expect("4 bytes")) as usize;
    let want_crc = u32::from_le_bytes(
        buf[offset + LEN_SZ..header_end]
            .try_into()
            .expect("4 bytes"),
    );

    let payload_end = header_end
        .checked_add(payload_len)
        .ok_or(FrameError::Truncated)?;
    if payload_end > buf.len() {
        return Err(FrameError::Truncated);
    }
    let payload = &buf[header_end..payload_end];
    if crc32fast::hash(payload) != want_crc {
        return Err(FrameError::BadCrc);
    }
    Ok(DecodedFrame {
        payload: payload.to_vec(),
        total_len: HEADER_SZ + payload_len,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let frame = encode(b"hello");
        let decoded = decode(&frame, 0).expect("valid frame");
        assert_eq!(decoded.payload, b"hello");
        assert_eq!(decoded.total_len, frame.len());
    }

    #[test]
    fn empty_payload_round_trips() {
        let frame = encode(b"");
        assert_eq!(frame.len(), HEADER_SZ);
        let decoded = decode(&frame, 0).expect("valid empty frame");
        assert!(decoded.payload.is_empty());
        assert_eq!(decoded.total_len, HEADER_SZ);
    }

    #[test]
    fn back_to_back_frames_decode_in_sequence() {
        let mut log = encode(b"one");
        let second_at = log.len();
        log.extend_from_slice(&encode(b"twotwo"));

        let a = decode(&log, 0).expect("first");
        assert_eq!(a.payload, b"one");
        assert_eq!(a.total_len, second_at);
        let b = decode(&log, second_at).expect("second");
        assert_eq!(b.payload, b"twotwo");
    }

    #[test]
    fn short_header_is_truncated() {
        let frame = encode(b"payload");
        // Only 3 bytes of the 8-byte header present.
        assert_eq!(decode(&frame[..3], 0), Err(FrameError::Truncated));
    }

    #[test]
    fn short_payload_is_truncated() {
        let frame = encode(b"0123456789");
        // Header intact, payload cut in half.
        let cut = HEADER_SZ + 5;
        assert_eq!(decode(&frame[..cut], 0), Err(FrameError::Truncated));
    }

    #[test]
    fn flipped_payload_bit_is_bad_crc() {
        let mut frame = encode(b"important");
        // Flip a bit inside the payload region, leaving length intact.
        frame[HEADER_SZ] ^= 0x01;
        assert_eq!(decode(&frame, 0), Err(FrameError::BadCrc));
    }
}
