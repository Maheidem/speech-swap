// swift-tools-version: 6.2
// FluidAudio runner for scripts/stt-lab.py (proc line protocol).
import PackageDescription

let package = Package(
    name: "fluidaudio-runner",
    platforms: [.macOS(.v14)],
    dependencies: [
        // traits: [] links ASR/VAD only (no TTS, no diarizer, no NeMo text normalization).
        .package(url: "https://github.com/FluidInference/FluidAudio.git", exact: "0.18.4", traits: [])
    ],
    targets: [
        .executableTarget(
            name: "fluidaudio-runner",
            dependencies: [.product(name: "FluidAudio", package: "FluidAudio")]
        )
    ]
)
