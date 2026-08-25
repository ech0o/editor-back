# ---------- Build ----------
FROM rust:1.89-slim AS builder
ENV SQLX_OFFLINE=true
WORKDIR /app

# 1. 安装构建依赖 (如果包含 C 库依赖如 openssl/sqlite，解除下面的注释)
# RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    python3 \
    cmake \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*
# 2. 预编译依赖项 (利用 Docker 缓存机制)
COPY Cargo.toml Cargo.lock ./
# SQLx 离线查询缓存
COPY .sqlx ./.sqlx
RUN mkdir src \
    && echo "fn main() {}" > src/main.rs \
    && cargo build --release \
    && rm -rf src



# 3. 编译实际源码
COPY src ./src
# touch main.rs 强制 cargo 识别到源码更新，防止跳过编译
RUN touch src/main.rs && cargo build --release

# 4. (可选) 剥离调试符号以大幅压缩体积
RUN strip /app/target/release/editor-back


# ---------- Runtime ----------
FROM debian:trixie-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/editor-back /usr/local/bin/editor-back

# 增加非 root 安全用户
#RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
#USER appuser

ENTRYPOINT ["/usr/local/bin/editor-back"]