# Build from the repository root: docker build -f docker/Dockerfile .
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY Directory.Build.props NuGet.config ./
COPY protos/ protos/
COPY src/StorageDemo.Core/ src/StorageDemo.Core/
COPY src/StorageDemo.Infrastructure/ src/StorageDemo.Infrastructure/
COPY src/StorageDemo.Api/ src/StorageDemo.Api/

RUN dotnet publish src/StorageDemo.Api/StorageDemo.Api.csproj -c Release -o /app

# FFmpeg ships in the image rather than coming from the distro, so nothing depends on the host's
# build.
RUN find /app/ffmpeg -mindepth 1 -maxdepth 1 ! -name linux-x64 -exec rm -rf {} + \
    && chmod +x /app/ffmpeg/linux-x64/ffmpeg /app/ffmpeg/linux-x64/ffprobe

# That packaged build is LGPL and has no libsrt, so it is replaced by one that does, matching what
# scripts/fetch-ffmpeg.sh puts in place for local runs.
ARG FFMPEG_BUILD=ffmpeg-n8.1-latest-linux64-gpl-shared-8.1
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl xz-utils ca-certificates \
    && curl -fsSL -o /tmp/ffmpeg.tar.xz \
        "https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/${FFMPEG_BUILD}.tar.xz" \
    && tar -xJf /tmp/ffmpeg.tar.xz -C /tmp \
    && cp -P /tmp/${FFMPEG_BUILD}/lib/*.so* /app/ffmpeg/linux-x64/ \
    && cp /tmp/${FFMPEG_BUILD}/bin/ffmpeg /tmp/${FFMPEG_BUILD}/bin/ffprobe /app/ffmpeg/linux-x64/ \
    && chmod +x /app/ffmpeg/linux-x64/ffmpeg /app/ffmpeg/linux-x64/ffprobe \
    && rm -rf /tmp/ffmpeg.tar.xz /tmp/${FFMPEG_BUILD} /var/lib/apt/lists/*

# RF-DETR Nano, exported to ONNX inside the image rather than expected on the host.
# The version pins matter more than usual and are kept identical to the script's: the .NET runner is
# tested against the tensor names and shapes this exact exporter version produces, and rfdetr has
# changed both before.
FROM python:3.12-slim AS rfdetr-export
WORKDIR /export
RUN python -m pip install --no-cache-dir --quiet \
        --extra-index-url https://download.pytorch.org/whl/cpu \
        rfdetr==1.10.1 torch==2.14.0+cpu torchvision==0.29.0+cpu onnxruntime==1.30.0 onnx==1.22.0
COPY scripts/export-rfdetr.py scripts/export-rfdetr.py
RUN mkdir -p models && python scripts/export-rfdetr.py

# Shared by the API and the worker: the same FFmpeg, the same user, the same loader path.
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS base
WORKDIR /app

# The bundled FFmpeg links against these at load time, for hardware acceleration it will never use
# inside a container.
# libsrt is different: unlike FFmpeg it is a stable C ABI with a soname, so the distro build is as
# good as any and there is nothing to match versions with.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libx11-6 libdrm2 libva2 libva-drm2 libvdpau1 libvpl2 \
        libsrt1.5-openssl \
    && rm -rf /var/lib/apt/lists/*

# Non-root. The image carries no credentials; everything comes from the environment at runtime.
RUN useradd --system --uid 64198 --create-home storagedemo \
    && mkdir -p /data \
    && chown -R storagedemo:storagedemo /data
USER storagedemo

# libav is loaded into this process by explicit path, but the libraries then resolve each other
# through the normal loader, which does not look next to the library it just loaded.
ENV LD_LIBRARY_PATH=/app/ffmpeg/linux-x64

# The detection worker (detection-plan.md D2), a separate deployment from the ingest pod:
#   docker build -f docker/Dockerfile --target worker -t storage-demo-worker .
# It carries the same libav and the ONNX Runtime natives the Infrastructure project references, and
# no ports: it subscribes to the API and posts back to it.
FROM build AS build-worker
COPY src/StorageDemo.Worker/ src/StorageDemo.Worker/
RUN dotnet publish src/StorageDemo.Worker/StorageDemo.Worker.csproj -c Release -p:OnnxRuntimeFlavor=Cuda -o /worker \
    && rm -rf /worker/ffmpeg \
    && cp -r /app/ffmpeg /worker/ffmpeg

FROM base AS worker
COPY --from=build-worker --chown=storagedemo:storagedemo /worker .
COPY --from=rfdetr-export --chown=storagedemo:storagedemo /export/models/rf-detr-nano.onnx models/rf-detr-nano.onnx
ENTRYPOINT ["dotnet", "StorageDemo.Worker.dll"]

# Last, so a build that names no target is still the API.
FROM base AS runtime
COPY --from=build --chown=storagedemo:storagedemo /app .

# Two cleartext ports, because HTTP/1.1 and HTTP/2 cannot share one without TLS to negotiate with:
# 8080 serves REST and the health probes, 5080 serves gRPC.
EXPOSE 8080
EXPOSE 5080

ENTRYPOINT ["dotnet", "StorageDemo.Api.dll"]
