# E-Connect Firmware Template

Standalone firmware template source for E-Connect DIY board builds.

This repository is intended to be published through GitHub Releases so the
E-Connect backend can install newer firmware template revisions without forcing
operators to upgrade the entire server package first.

## Repository Layout

- `platformio.ini`: baseline PlatformIO configuration
- `include/firmware_revision.h`: developer-managed firmware revision
- `include/secrets.h`: compile-time secret placeholders
- `src/`: core firmware source
- `boards/`: custom board definitions

## Release Contract

- Release tags should match the firmware revision in `include/firmware_revision.h`
- The backend downloads the GitHub release source tarball and expects the
  template files at the repository root
- Keep `platformio.ini`, `include/firmware_revision.h`, and `src/main.cpp`
  present in every release
