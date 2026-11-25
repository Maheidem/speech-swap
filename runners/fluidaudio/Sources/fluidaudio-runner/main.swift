// FluidAudio (Parakeet TDT) runner for scripts/stt-lab.py.
// Protocol: see the docstring of scripts/stt-lab.py. stdout carries only protocol lines.
// Usage: fluidaudio-runner [--models DIR] [--version v3|ultra] [--vocab FILE] [--ctc-dir DIR]
//   --vocab FILE  one term per line; turns on CTC keyword boost (loads the CTC 110M model).
import FluidAudio
import Foundation

// Keep the real stdout for protocol lines. Send all other prints to stderr.
let protoFD = dup(1)
dup2(2, 1)
let proto = FileHandle(fileDescriptor: protoFD)

func emit(_ obj: [String: Any]) {
    var data = (try? JSONSerialization.data(withJSONObject: obj, options: [])) ?? Data("{}".utf8)
    data.append(0x0A)
    proto.write(data)
}

func log(_ s: String) { FileHandle.standardError.write(Data((s + "\n").utf8)) }

func arg(_ name: String) -> String? {
    let a = CommandLine.arguments
    guard let i = a.firstIndex(of: name), i + 1 < a.count else { return nil }
    return a[i + 1]
}

func ms(_ t0: Date) -> Double { Date().timeIntervalSince(t0) * 1000 }

let modelsDir = arg("--models").map { URL(fileURLWithPath: ($0 as NSString).expandingTildeInPath) }
let version: AsrModelVersion = arg("--version") == "ultra" ? .ultra : .v3
let vocabPath = arg("--vocab").map { ($0 as NSString).expandingTildeInPath }
let ctcDir = arg("--ctc-dir").map { URL(fileURLWithPath: ($0 as NSString).expandingTildeInPath) }

let t0 = Date()
let asr = AsrManager(config: .default)
var session: VocabularyBoostingSession? = nil
do {
    let models = try await AsrModels.downloadAndLoad(to: modelsDir, version: version)
    try await asr.loadModels(models)
    if let vocabPath {
        let vocab = try CustomVocabularyContext.loadFromSimpleFormat(from: URL(fileURLWithPath: vocabPath))
        let ctc = try await CtcModels.downloadAndLoad(to: ctcDir, variant: .ctc110m)
        session = try await VocabularyBoostingSession(vocabulary: vocab, ctcModels: ctc)
        log("vocab boost on: \(vocab.terms.count) terms")
    }
} catch {
    emit(["error": "load: \(error)"])
    exit(1)
}
emit(["ready": true, "load_ms": Int(ms(t0))])

let converter = AudioConverter()
while let line = readLine() {
    guard !line.isEmpty, let d = line.data(using: .utf8),
        let msg = try? JSONSerialization.jsonObject(with: d) as? [String: Any],
        let file = msg["file"] as? String
    else { continue }
    if msg["stream"] as? Bool == true {
        emit(["file": file, "error": "stream not supported by this runner"])
        continue
    }
    let lang = (msg["language"] as? String).flatMap { Language(rawValue: $0) }
    do {
        let t = Date()
        let samples = try converter.resampleAudioFile(path: file)
        var state = TdtDecoderState.make(decoderLayers: await asr.decoderLayerCount)
        let r = try await asr.transcribe(samples, decoderState: &state, language: lang)
        var text = r.text
        var extra: [String: Any] = [:]
        if let session {
            let out = await session.rescore(
                text: r.text, tokenTimings: r.tokenTimings ?? [], audioSamples: samples)
            if let out {
                text = out.text
                extra["vocab_detected"] = out.detectedTerms
            }
        }
        let dur = Double(samples.count) / 16000
        var res: [String: Any] = [
            "file": file, "text": text.trimmingCharacters(in: .whitespacesAndNewlines),
            "segments": [["start": 0.0, "end": dur, "text": text]], "engine_ms": ms(t),
        ]
        res.merge(extra) { a, _ in a }
        emit(res)
    } catch {
        emit(["file": file, "error": "\(error)"])
    }
}
