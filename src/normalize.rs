//! Input normalization: the language field and the audio file.
//! Each client sends a different form. Each engine gets one form.

use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

/// Language names and their ISO-639-1 codes (ISO-639-3 where there is no
/// two-letter code). The list has the 30 languages of Qwen3-ASR.
const NAMES: &[(&str, &str)] = &[
    ("arabic", "ar"),
    ("cantonese", "yue"),
    ("chinese", "zh"),
    ("mandarin", "zh"),
    ("czech", "cs"),
    ("danish", "da"),
    ("dutch", "nl"),
    ("english", "en"),
    ("filipino", "fil"),
    ("tagalog", "fil"),
    ("finnish", "fi"),
    ("french", "fr"),
    ("français", "fr"),
    ("german", "de"),
    ("deutsch", "de"),
    ("greek", "el"),
    ("hindi", "hi"),
    ("hungarian", "hu"),
    ("indonesian", "id"),
    ("italian", "it"),
    ("italiano", "it"),
    ("japanese", "ja"),
    ("korean", "ko"),
    ("macedonian", "mk"),
    ("malay", "ms"),
    ("persian", "fa"),
    ("polish", "pl"),
    ("portuguese", "pt"),
    ("português", "pt"),
    ("portugues", "pt"),
    ("romanian", "ro"),
    ("russian", "ru"),
    ("spanish", "es"),
    ("español", "es"),
    ("swedish", "sv"),
    ("thai", "th"),
    ("turkish", "tr"),
    ("vietnamese", "vi"),
];

/// Changes a language value into a code. `None` means "detect the language".
///
/// Accepted forms: a code (`pt`), a BCP-47 tag (`pt-BR`, `pt_BR`), a full
/// name in any case (`Portuguese`, `Portuguese (Brazil)`), and the names in
/// `extra`. An empty value, `auto` and an unknown name give `None`.
pub fn language(raw: Option<&str>, extra: &HashMap<String, String>) -> Option<String> {
    let value = raw?.trim().to_lowercase();
    if value.is_empty() || value == "auto" {
        return None;
    }
    let lookup = |name: &str| {
        extra
            .iter()
            .find(|(k, _)| k.to_lowercase() == name)
            .map(|(_, code)| code.to_lowercase())
            .or_else(|| NAMES.iter().find(|(n, _)| *n == name).map(|(_, code)| code.to_string()))
    };
    if let Some(code) = lookup(&value) {
        return Some(code);
    }
    // "portuguese (brazil)" -> "portuguese"
    if let Some(code) = value.split('(').next().and_then(|name| lookup(name.trim())) {
        return Some(code);
    }
    // "pt-br" -> "pt"
    let primary = value.split(['-', '_']).next().unwrap_or("");
    if (2..=3).contains(&primary.len()) && primary.chars().all(|c| c.is_ascii_lowercase()) {
        return Some(primary.to_string());
    }
    None
}

/// Decodes standard base64. Line breaks and spaces are skipped. `None` means
/// the text is not valid base64.
pub fn base64_decode(input: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(input.len() * 3 / 4);
    let (mut acc, mut bits, mut pad) = (0u32, 0u32, 0usize);
    for c in input.bytes() {
        let v = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            b'=' => {
                pad += 1;
                continue;
            }
            b'\n' | b'\r' | b' ' => continue,
            _ => return None,
        };
        if pad > 0 {
            return None;
        }
        acc = acc << 6 | v as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }
    (pad <= 2).then_some(out)
}

/// The position of the samples in a WAV file.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WavData {
    pub offset: u64,
    pub len: u64,
}

impl WavData {
    /// The duration of 16 kHz mono 16-bit audio.
    pub fn seconds(&self) -> f64 {
        self.len as f64 / 32000.0
    }
}

/// Finds the `data` chunk of a WAV file. ffmpeg can write other chunks
/// before it, so the hub reads the chunk list.
pub fn wav_data(path: &Path) -> std::io::Result<WavData> {
    let bad = |why: &str| std::io::Error::new(std::io::ErrorKind::InvalidData, why.to_string());
    let mut file = std::fs::File::open(path)?;
    let file_len = file.metadata()?.len();
    let mut head = [0u8; 12];
    file.read_exact(&mut head)?;
    if &head[0..4] != b"RIFF" || &head[8..12] != b"WAVE" {
        return Err(bad("not a WAV file"));
    }
    let mut pos = 12u64;
    loop {
        let mut chunk = [0u8; 8];
        file.seek(SeekFrom::Start(pos))?;
        file.read_exact(&mut chunk).map_err(|_| bad("the WAV file has no data chunk"))?;
        let size = u32::from_le_bytes([chunk[4], chunk[5], chunk[6], chunk[7]]) as u64;
        if &chunk[0..4] == b"data" {
            let offset = pos + 8;
            // A size of 0 or 0xFFFFFFFF means "to the end of the file".
            let rest = file_len - offset;
            let len = if size == 0 || size > rest { rest } else { size };
            return Ok(WavData { offset, len: len & !1 });
        }
        pos += 8 + size + (size & 1);
    }
}

/// Converts any audio file to 16 kHz mono 16-bit WAV with an ffmpeg child.
/// On failure the error text is the end of the ffmpeg output.
pub async fn transcode(ffmpeg: &str, input: &Path, output: &Path, timeout: Duration) -> Result<(), String> {
    let mut cmd = tokio::process::Command::new(ffmpeg);
    cmd.args(["-nostdin", "-hide_banner", "-loglevel", "error", "-y", "-i"])
        .arg(input)
        .args(["-vn", "-ac", "1", "-ar", "16000", "-c:a", "pcm_s16le", "-f", "wav"])
        .arg(output)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let child = cmd.spawn().map_err(|e| format!("ffmpeg did not start: {e}"))?;
    match tokio::time::timeout(timeout, child.wait_with_output()).await {
        Err(_) => Err(format!("ffmpeg did not finish in {} s", timeout.as_secs())),
        Ok(Err(e)) => Err(format!("ffmpeg failed: {e}")),
        Ok(Ok(out)) if out.status.success() => Ok(()),
        Ok(Ok(out)) => {
            let text = String::from_utf8_lossy(&out.stderr);
            // Keep the last line only. It names the cause, and it has no temporary path in most cases.
            let last = text.lines().rev().find(|l| !l.trim().is_empty()).unwrap_or("no output");
            Err(last.chars().take(200).collect())
        }
    }
}

/// Converts raw PCM (any rate, width 1, 2 or 4 bytes, any channel count) to
/// 16 kHz mono 16-bit WAV with an ffmpeg child.
pub async fn transcode_raw(ffmpeg: &str, input: &Path, output: &Path, rate: u32, width: u32, channels: u32, timeout: Duration) -> Result<(), String> {
    let format = match width {
        1 => "u8",
        2 => "s16le",
        4 => "s32le",
        _ => return Err(format!("sample width {width} is not supported")),
    };
    let mut cmd = tokio::process::Command::new(ffmpeg);
    cmd.args(["-nostdin", "-hide_banner", "-loglevel", "error", "-y", "-f", format, "-ar"])
        .arg(rate.to_string())
        .arg("-ac")
        .arg(channels.to_string())
        .arg("-i")
        .arg(input)
        .args(["-ac", "1", "-ar", "16000", "-c:a", "pcm_s16le", "-f", "wav"])
        .arg(output)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let child = cmd.spawn().map_err(|e| format!("ffmpeg did not start: {e}"))?;
    match tokio::time::timeout(timeout, child.wait_with_output()).await {
        Err(_) => Err(format!("ffmpeg did not finish in {} s", timeout.as_secs())),
        Ok(Err(e)) => Err(format!("ffmpeg failed: {e}")),
        Ok(Ok(out)) if out.status.success() => Ok(()),
        Ok(Ok(out)) => Err(String::from_utf8_lossy(&out.stderr).lines().last().unwrap_or("no output").chars().take(200).collect()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn lang(raw: &str) -> Option<String> {
        language(Some(raw), &HashMap::new())
    }

    #[test]
    fn a_code_stays_a_code() {
        assert_eq!(lang("pt").as_deref(), Some("pt"));
        assert_eq!(lang(" EN ").as_deref(), Some("en"));
        assert_eq!(lang("yue").as_deref(), Some("yue"));
    }

    #[test]
    fn a_bcp47_tag_keeps_the_primary_subtag() {
        assert_eq!(lang("pt-BR").as_deref(), Some("pt"));
        assert_eq!(lang("pt_BR").as_deref(), Some("pt"));
        assert_eq!(lang("zh-Hans-CN").as_deref(), Some("zh"));
    }

    #[test]
    fn a_full_name_becomes_a_code() {
        assert_eq!(lang("English").as_deref(), Some("en"));
        assert_eq!(lang("PORTUGUESE").as_deref(), Some("pt"));
        assert_eq!(lang("Português").as_deref(), Some("pt"));
        assert_eq!(lang("Portuguese (Brazil)").as_deref(), Some("pt"));
    }

    #[test]
    fn auto_empty_absent_and_unknown_mean_detect() {
        assert_eq!(lang("auto"), None);
        assert_eq!(lang("  "), None);
        assert_eq!(language(None, &HashMap::new()), None);
        assert_eq!(lang("Klingon"), None);
        assert_eq!(lang("12"), None);
    }

    #[test]
    fn base64_decodes_known_values() {
        for (text, expect) in [("", ""), ("Zg==", "f"), ("Zm8=", "fo"), ("Zm9v", "foo"), ("Zm9vYmFy", "foobar"), ("Zm9v\nYmFy", "foobar")] {
            assert_eq!(base64_decode(text).unwrap(), expect.as_bytes());
        }
        assert!(base64_decode("Zm9v!").is_none());
        assert!(base64_decode("Zg==Zg").is_none());
    }

    #[test]
    fn the_config_adds_names() {
        let extra = HashMap::from([("Brazilian Portuguese".to_string(), "pt".to_string())]);
        assert_eq!(language(Some("brazilian portuguese"), &extra).as_deref(), Some("pt"));
    }

    fn wav_with(chunks: &[(&[u8; 4], &[u8])], name: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!("ssw-test-{}-{name}.wav", std::process::id()));
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(b"RIFF\0\0\0\0WAVE").unwrap();
        for (id, body) in chunks {
            f.write_all(*id).unwrap();
            f.write_all(&(body.len() as u32).to_le_bytes()).unwrap();
            f.write_all(body).unwrap();
            if body.len() % 2 == 1 {
                f.write_all(&[0]).unwrap();
            }
        }
        path
    }

    #[test]
    fn the_data_chunk_is_found_after_other_chunks() {
        let path = wav_with(&[(b"fmt ", &[0; 16]), (b"LIST", &[1; 27]), (b"data", &[2; 64000])], "list");
        let data = wav_data(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        assert_eq!(data, WavData { offset: 12 + 24 + 36 + 8, len: 64000 });
        assert_eq!(data.seconds(), 2.0);
    }

    #[test]
    fn a_plain_header_has_44_bytes() {
        let path = wav_with(&[(b"fmt ", &[0; 16]), (b"data", &[2; 320])], "plain");
        let data = wav_data(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        assert_eq!(data, WavData { offset: 44, len: 320 });
    }

    #[test]
    fn a_file_that_is_not_wav_is_an_error() {
        let path = std::env::temp_dir().join(format!("ssw-test-{}-bad.wav", std::process::id()));
        std::fs::write(&path, b"OggS and more bytes here").unwrap();
        assert!(wav_data(&path).is_err());
        std::fs::remove_file(&path).unwrap();
    }
}
