# Validation-only Linux SDK. Production payloads contain no test driver or X server.
FROM rust:1.88-bookworm AS rusttool
FROM ubuntu:22.04
ENV DEBIAN_FRONTEND=noninteractive
ENV CARGO_HOME=/usr/local/cargo RUSTUP_HOME=/usr/local/rustup
ENV PATH=/usr/local/cargo/bin:$PATH
COPY --from=rusttool /usr/local/rustup /usr/local/rustup
COPY --from=rusttool /usr/local/cargo /usr/local/cargo
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential pkg-config ca-certificates curl libssl-dev nodejs \
    libwebkit2gtk-4.1-dev libxdo-dev webkit2gtk-driver \
    xvfb xauth xdotool imagemagick dbus-x11 \
    xdg-desktop-portal xdg-desktop-portal-gtk fonts-dejavu-core python3 \
    && rm -rf /var/lib/apt/lists/*
RUN rustc --version && cargo --version && rustup component add rustfmt
RUN CARGO_BUILD_JOBS=2 cargo install tauri-driver --version 2.0.6 --locked
