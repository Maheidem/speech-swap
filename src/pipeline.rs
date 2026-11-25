//! The parts of the STT to align pipeline that need no engine:
//! the RMS window cutter, the merge of window results, the cues and the
//! SRT and VTT text. All functions are pure, except `frame_energies`, which
//! reads a file.

use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::config::Chunking;

/// The samples in one second of the audio that the hub uses (16 kHz mono).
pub const RATE: usize = 16000;

/// Reads the mean energy of each frame of 16-bit samples. The file is read
/// in pieces: the hub never holds the audio in memory.
pub fn frame_energies(path: &Path, offset: u64, len: u64, frame_samples: usize) -> std::io::Result<Vec<f32>> {
    let mut file = std::io::BufReader::new(std::fs::File::open(path)?);
    file.seek(SeekFrom::Start(offset))?;
    let total = (len / 2) as usize;
    let mut out = Vec::with_capacity(total / frame_samples + 1);
    let mut buf = vec![0u8; frame_samples * 2];
    let mut done = 0;
    while done < total {
        let n = (total - done).min(frame_samples);
        file.read_exact(&mut buf[..n * 2])?;
        let sum: f64 = buf[..n * 2].chunks_exact(2).map(|b| (i16::from_le_bytes([b[0], b[1]]) as f64).powi(2)).sum();
        out.push((sum / n as f64) as f32);
        done += n;
    }
    Ok(out)
}

/// Cuts the audio into windows of at most `window_max_s`. A cut falls inside
/// the span `window_min_s` to `window_max_s` of the window, at the centre of
/// the quietest span of `cut_window_ms`. `energy` has one value for each
/// frame. The result is a list of `(first frame, end frame)` pairs.
pub fn cut_windows(energy: &[f32], ck: &Chunking) -> Vec<(usize, usize)> {
    let per_s = 1000.0 / ck.frame_ms as f64;
    let (min, max) = ((ck.window_min_s * per_s) as usize, (ck.window_max_s * per_s) as usize);
    let span = ((ck.cut_window_ms / ck.frame_ms) as usize).max(1);
    // prefix[i] = the sum of the energy of frames 0..i
    let mut prefix = Vec::with_capacity(energy.len() + 1);
    prefix.push(0.0f64);
    for e in energy {
        prefix.push(prefix.last().unwrap() + *e as f64);
    }
    let mut windows = Vec::new();
    let mut pos = 0;
    while energy.len() - pos > max {
        // The quiet span lies fully inside [pos + min, pos + max]. Equal sums
        // (for example digital silence) give the middle of the equal range.
        let sums: Vec<f64> = (pos + min..=pos + max - span).map(|s| prefix[s + span] - prefix[s]).collect();
        let lowest = sums.iter().cloned().fold(f64::MAX, f64::min);
        let tied = |s: &f64| *s <= lowest + 1e-6 + lowest.abs() * 1e-9;
        let first = sums.iter().position(tied).expect("one value is the lowest");
        let last = sums.iter().rposition(tied).expect("one value is the lowest");
        let cut = pos + min + (first + last) / 2 + span / 2;
        windows.push((pos, cut));
        pos = cut;
    }
    if pos < energy.len() || windows.is_empty() {
        windows.push((pos, energy.len()));
    }
    windows
}

/// One aligned word. The times are in seconds from the start of the file.
#[derive(Debug, Clone, PartialEq)]
pub struct Word {
    pub text: String,
    pub start: f64,
    pub end: f64,
}

/// The result of one window.
#[derive(Debug, Clone, Default)]
pub struct WindowResult {
    /// The start of the window in seconds.
    pub start_s: f64,
    pub text: String,
    pub language: Option<String>,
    /// The words with times relative to the start of the window.
    pub words: Vec<Word>,
}

/// The indexes of the windows that need an align job: the windows with text.
pub fn align_steps(texts: &[&str]) -> Vec<usize> {
    texts.iter().enumerate().filter(|(_, t)| !t.trim().is_empty()).map(|(i, _)| i).collect()
}

/// Puts the times of all windows on one time line. Words that the aligner
/// returns after the end of the window stay as they are.
pub fn merge(results: &[WindowResult]) -> (String, Vec<Word>) {
    let text = results.iter().map(|r| r.text.trim()).filter(|t| !t.is_empty()).collect::<Vec<_>>().join(" ");
    let words = results
        .iter()
        .flat_map(|r| r.words.iter().map(move |w| Word { text: w.text.clone(), start: r.start_s + w.start, end: r.start_s + w.end }))
        .collect();
    (text, words)
}

/// The letters and digits of a token. The aligner drops all other marks,
/// also inside a token ("3.8" becomes "38").
fn key(token: &str) -> String {
    token.chars().filter(|c| c.is_alphanumeric()).flat_map(char::to_lowercase).collect()
}

/// The aligner drops the punctuation. This function gives the original
/// tokens back (with their marks) when the tokens of the text match the
/// words one to one. Otherwise it returns `None`.
pub fn tokens_with_marks(text: &str, words: &[Word]) -> Option<Vec<String>> {
    let tokens: Vec<&str> = text.split_whitespace().filter(|t| !key(t).is_empty()).collect();
    (tokens.len() == words.len() && tokens.iter().zip(words).all(|(t, w)| key(t) == key(&w.text)))
        .then(|| tokens.into_iter().map(str::to_string).collect())
}

/// A subtitle cue or a segment.
#[derive(Debug, Clone, PartialEq)]
pub struct Cue {
    pub start: f64,
    pub end: f64,
    pub text: String,
}

const CUE_MAX_CHARS: usize = 84;
const CUE_MAX_S: f64 = 6.0;
const CUE_GAP_S: f64 = 1.0;

fn ends_sentence(token: &str) -> bool {
    token.trim_end_matches(['"', '\'', ')', ']', '»']).ends_with(['.', '?', '!', '…'])
}

/// Groups words into cues. A cue ends at the end of a sentence, at a pause
/// of 1 s or more, or when it gets too long (84 characters or 6 s).
/// `display` holds one text for each word (with or without marks).
pub fn cues(words: &[Word], display: &[String]) -> Vec<Cue> {
    let mut out: Vec<Cue> = Vec::new();
    let mut current: Option<Cue> = None;
    for (i, w) in words.iter().enumerate() {
        let shown = display.get(i).map(String::as_str).unwrap_or(&w.text);
        let cue = current.get_or_insert_with(|| Cue { start: w.start, end: w.end, text: String::new() });
        if !cue.text.is_empty() {
            cue.text.push(' ');
        }
        cue.text.push_str(shown);
        cue.end = w.end.max(cue.end);
        let next = words.get(i + 1);
        let split = next.is_none()
            || ends_sentence(shown)
            || next.is_some_and(|n| n.start - w.end >= CUE_GAP_S)
            || cue.text.chars().count() >= CUE_MAX_CHARS
            || cue.end - cue.start >= CUE_MAX_S;
        if split {
            out.extend(current.take());
        }
    }
    out
}

fn stamp(seconds: f64, sep: char) -> String {
    let ms = (seconds.max(0.0) * 1000.0).round() as u64;
    format!("{:02}:{:02}:{:02}{sep}{:03}", ms / 3_600_000, ms / 60_000 % 60, ms / 1000 % 60, ms % 1000)
}

pub fn srt(cues: &[Cue]) -> String {
    let mut out = String::new();
    for (i, c) in cues.iter().enumerate() {
        out.push_str(&format!("{}\n{} --> {}\n{}\n\n", i + 1, stamp(c.start, ','), stamp(c.end, ','), c.text));
    }
    out
}

pub fn vtt(cues: &[Cue]) -> String {
    let mut out = String::from("WEBVTT\n\n");
    for c in cues {
        out.push_str(&format!("{} --> {}\n{}\n\n", stamp(c.start, '.'), stamp(c.end, '.'), c.text));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn word(text: &str, start: f64, end: f64) -> Word {
        Word { text: text.into(), start, end }
    }

    fn ck() -> Chunking {
        Chunking::default()
    }

    /// 50 frames per second. A loud signal with quiet gaps at the given seconds.
    fn curve(seconds: f64, quiet: &[(f64, f64)]) -> Vec<f32> {
        (0..(seconds * 50.0) as usize)
            .map(|i| {
                let t = i as f64 / 50.0;
                if quiet.iter().any(|(a, b)| t >= *a && t < *b) { 1.0 } else { 1000.0 }
            })
            .collect()
    }

    #[test]
    fn a_short_file_is_one_window() {
        let e = curve(30.0, &[]);
        assert_eq!(cut_windows(&e, &ck()), [(0, 1500)]);
        let e = curve(3.0, &[]);
        assert_eq!(cut_windows(&e, &ck()), [(0, 150)]);
        assert_eq!(cut_windows(&[], &ck()), [(0, 0)]);
    }

    #[test]
    fn the_cut_falls_in_the_quiet_span() {
        // A quiet gap from 26.0 s to 26.4 s. The cut is at its centre, 26.2 s = frame 1310.
        let e = curve(40.0, &[(26.0, 26.4)]);
        let w = cut_windows(&e, &ck());
        assert_eq!(w, [(0, 1310), (1310, 2000)]);
    }

    #[test]
    fn a_gap_outside_the_range_is_ignored() {
        // The gap at 10 s is before the 20 s limit. The gap at 33 s is after the 30 s limit.
        let e = curve(40.0, &[(10.0, 11.0), (33.0, 34.0)]);
        let w = cut_windows(&e, &ck());
        let cut = w[0].1;
        assert!((1000..=1500).contains(&cut), "cut {cut}");
        assert_eq!(w.len(), 2);
    }

    #[test]
    fn every_window_is_at_most_30_s_and_they_cover_the_file() {
        let e = curve(185.0, &[(27.0, 27.3), (55.0, 55.5), (84.0, 84.2), (120.0, 121.0), (150.0, 150.3)]);
        let w = cut_windows(&e, &ck());
        assert_eq!(w[0].0, 0);
        assert_eq!(w.last().unwrap().1, e.len());
        for pair in w.windows(2) {
            assert_eq!(pair[0].1, pair[1].0);
        }
        for (a, b) in &w {
            assert!(b - a <= 1500 && b > a, "window {a}..{b}");
        }
        assert!(w.len() >= 7);
    }

    #[test]
    fn a_file_with_no_quiet_span_still_cuts_at_the_limits() {
        let e = vec![5.0f32; 50 * 100];
        let w = cut_windows(&e, &ck());
        assert!(w.iter().all(|(a, b)| b - a <= 1500));
        assert_eq!(w.last().unwrap().1, 5000);
    }

    #[test]
    fn frame_energies_reads_the_samples() {
        let path = std::env::temp_dir().join(format!("ssw-test-{}-rms.raw", std::process::id()));
        let mut bytes = vec![0u8; 8]; // 8 bytes of header, skipped by the offset
        for i in 0..640i16 {
            bytes.extend_from_slice(&(if i < 320 { 100i16 } else { 0 }).to_le_bytes());
        }
        std::fs::write(&path, bytes).unwrap();
        let e = frame_energies(&path, 8, 1280, 320).unwrap();
        std::fs::remove_file(&path).unwrap();
        assert_eq!(e.len(), 2);
        assert_eq!(e[0], 10000.0);
        assert_eq!(e[1], 0.0);
    }

    #[test]
    fn windows_without_text_get_no_align_step() {
        assert_eq!(align_steps(&["a b", "", "  ", "c"]), [0, 3]);
        assert_eq!(align_steps(&[]), Vec::<usize>::new());
    }

    #[test]
    fn merge_adds_the_window_start_to_the_times() {
        let results = vec![
            WindowResult { start_s: 0.0, text: "um dois".into(), language: None, words: vec![word("um", 0.5, 0.9), word("dois", 1.0, 1.4)] },
            WindowResult { start_s: 24.5, text: " ".into(), language: None, words: vec![] },
            WindowResult { start_s: 51.2, text: "três".into(), language: None, words: vec![word("três", 0.25, 0.75)] },
        ];
        let (text, words) = merge(&results);
        assert_eq!(text, "um dois três");
        assert_eq!(words.len(), 3);
        assert_eq!((words[2].start, words[2].end), (51.45, 51.95));
        assert_eq!((words[0].start, words[1].end), (0.5, 1.4));
    }

    #[test]
    fn marks_come_back_when_the_counts_match() {
        let words = vec![word("Cancela", 0.0, 1.0), word("o", 1.0, 1.1), word("timer", 1.1, 1.5), word("atual", 1.5, 2.0)];
        let got = tokens_with_marks("Cancela o timer, atual.", &words).unwrap();
        assert_eq!(got, ["Cancela", "o", "timer,", "atual."]);
        // A token that is only a mark has no word.
        let got = tokens_with_marks("Cancela - o timer, atual.", &words).unwrap();
        assert_eq!(got.len(), 4);
        // The aligner drops marks inside a token too.
        let nums = vec![word("122400", 0.0, 1.0), word("Segundo", 1.0, 2.0)];
        assert_eq!(tokens_with_marks("122.400. Segundo,", &nums).unwrap(), ["122.400.", "Segundo,"]);
        // The counts differ: no result.
        assert!(tokens_with_marks("Cancela o timer", &words).is_none());
    }

    #[test]
    fn cues_end_at_sentences_pauses_and_limits() {
        let words = vec![
            word("Oi", 0.0, 0.3),
            word("tudo", 0.4, 0.7),
            word("bem?", 0.8, 1.2),
            word("Sim", 1.3, 1.6),
            word("sim", 1.7, 2.0),
            word("depois", 5.0, 5.4),
            word("do", 5.5, 5.6),
            word("fim", 5.7, 6.0),
        ];
        let shown: Vec<String> = words.iter().map(|w| w.text.clone()).collect();
        let c = cues(&words, &shown);
        assert_eq!(c.len(), 3);
        assert_eq!(c[0], Cue { start: 0.0, end: 1.2, text: "Oi tudo bem?".into() });
        // A pause of 3 s ends the second cue.
        assert_eq!(c[1].text, "Sim sim");
        assert_eq!((c[2].start, c[2].end), (5.0, 6.0));
    }

    #[test]
    fn a_long_cue_is_split() {
        let words: Vec<Word> = (0..30).map(|i| word("palavra", i as f64 * 0.5, i as f64 * 0.5 + 0.4)).collect();
        let shown: Vec<String> = words.iter().map(|w| w.text.clone()).collect();
        let c = cues(&words, &shown);
        assert!(c.len() >= 3);
        assert!(c.iter().all(|c| c.end - c.start <= 6.4));
    }

    #[test]
    fn srt_and_vtt_use_their_own_marks() {
        let c = vec![Cue { start: 0.0, end: 1.5, text: "Oi.".into() }, Cue { start: 3723.456, end: 3725.0, text: "Fim.".into() }];
        assert_eq!(srt(&c), "1\n00:00:00,000 --> 00:00:01,500\nOi.\n\n2\n01:02:03,456 --> 01:02:05,000\nFim.\n\n");
        assert_eq!(vtt(&c), "WEBVTT\n\n00:00:00.000 --> 00:00:01.500\nOi.\n\n01:02:03.456 --> 01:02:05.000\nFim.\n\n");
    }
}
