use std::fs::File;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{anyhow, bail, Context, Result};
use hyper::http;
use containerd_shim_wasm::container::{
    Engine, Entrypoint, Instance, RuntimeContext, Stdio, WasmBinaryType,
};
use wasi_common::I32Exit;
use wasmtime::component::{self as wasmtime_component, Component, InstancePre};
use wasmtime::{Module, Store, StoreLimits};
use wasmtime_wasi::preview2::{self as wasi_preview2, Table, WasiCtx, WasiView};
use wasmtime_wasi::{self as wasi_preview1, Dir, WasiCtxBuilder};
use wasmtime_wasi_http::io::TokioIo;
use wasmtime_wasi_http::{
    bindings::http::types as http_types, body::HyperOutgoingBody, hyper_response_error,
    WasiHttpCtx, WasiHttpView,
};

pub type WasmtimeInstance = Instance<WasmtimeEngine>;

#[derive(Clone)]
pub struct WasmtimeEngine {
    engine: wasmtime::Engine,
}

impl Default for WasmtimeEngine {
    fn default() -> Self {
        let mut config = wasmtime::Config::new();
        config.wasm_component_model(true); // enable component linking
        Self {
            engine: wasmtime::Engine::new(&config)
                .context("failed to create wasmtime engine")
                .unwrap(),
        }
    }
}

impl Engine for WasmtimeEngine {
    fn name() -> &'static str {
        "wasmtimeserve"
    }

    fn run_wasi(&self, ctx: &impl RuntimeContext, stdio: Stdio) -> Result<i32> {
        log::info!("setting up wasi");
        let envs: Vec<_> = std::env::vars().collect();
        let Entrypoint {
            source,
            func,
            arg0: _,
            name: _,
        } = ctx.entrypoint();

        stdio.redirect()?;

        let wasm_binary = source.into_wasm_binary()?;

        log::info!("building wasi context");

        let status = match WasmBinaryType::from_bytes(&wasm_binary) {
            Some(WasmBinaryType::Module) => bail!("module not supported"),
            Some(WasmBinaryType::Component) => self.execute_component_serve(ctx, wasm_binary, func, envs)?,
            None => bail!("not a valid wasm binary format"),
        };

        let status = status.map(|_| 0).or_else(|err| {
            match err.downcast_ref::<I32Exit>() {
                // On Windows, exit status 3 indicates an abort (see below),
                // so return 1 indicating a non-zero status to avoid ambiguity.
                #[cfg(windows)]
                Some(I32Exit(3..)) => Ok(1),
                Some(I32Exit(status)) => Ok(*status),
                _ => Err(err),
            }
        })?;

        Ok(status)
    }
}

impl WasmtimeEngine {
    fn execute_component_serve(
        &self,
        ctx: &impl RuntimeContext,
        wasm_binary: Vec<u8>,
        func: String,
        envs: Vec<(String, String)>
    ) -> Result<std::prelude::v1::Result<(), anyhow::Error>, anyhow::Error> {
        log::debug!("loading wasm component");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_time()
            .enable_io()
            .build()?;

        let args = ctx.args().to_vec();

        let engine = self.engine.clone();
        runtime.block_on(async move {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    Ok::<_, anyhow::Error>(())
                }

                res = serve(engine, wasm_binary, args, envs) => {
                    res
                }
            }
        })?;

        Ok(Ok(()))
    }

}

async fn serve(engine: wasmtime::Engine, wasm_binary: Vec<u8>, args: Vec<String>, envs: Vec<(String, String)>) -> Result<()> {
    let component = Component::from_binary(&engine, &wasm_binary)?;
    let mut linker = wasmtime_component::Linker::new(&engine);

    wasi_preview2::command::add_to_linker(&mut linker)?;
    wasmtime_wasi_http::proxy::add_only_http_to_linker(&mut linker)?;

    let instance = linker.instantiate_pre(&component)?;

    use hyper::server::conn::http1;

    const DEFAULT_ADDR: std::net::SocketAddr = std::net::SocketAddr::new(
        std::net::IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0)),
        8080,
    );

    let listener = tokio::net::TcpListener::bind(DEFAULT_ADDR).await?;

    eprintln!("Serving HTTP on http://{}/", listener.local_addr()?);

    log::info!("Listening on {}", DEFAULT_ADDR);

    let handler = ProxyHandler::new(engine, instance, args, envs);

    loop {
        let (stream, _) = listener.accept().await?;
        let stream = TokioIo::new(stream);
        let h = handler.clone();
        tokio::task::spawn(async move {
            if let Err(e) = http1::Builder::new()
                .keep_alive(true)
                .serve_connection(stream, h)
                .await
            {
                eprintln!("error: {e:?}");
            }
        });
    }
}

struct Host {
    table: Table,
    ctx: WasiCtx,
    http: WasiHttpCtx,

    limits: StoreLimits,
}

impl WasiView for Host {
    fn table(&self) -> &Table {
        &self.table
    }

    fn table_mut(&mut self) -> &mut Table {
        &mut self.table
    }

    fn ctx(&self) -> &WasiCtx {
        &self.ctx
    }

    fn ctx_mut(&mut self) -> &mut WasiCtx {
        &mut self.ctx
    }
}

impl WasiHttpView for Host {
    fn table(&mut self) -> &mut Table {
        &mut self.table
    }

    fn ctx(&mut self) -> &mut WasiHttpCtx {
        &mut self.http
    }
}

struct ProxyHandlerInner {
    engine: wasmtime::Engine,
    args: Vec<String>,
    instance_pre: InstancePre<Host>,
    next_id: AtomicU64,
    envs: Vec<(String, String)>,
}

impl ProxyHandlerInner {
    fn next_req_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }
}

#[derive(Clone)]
struct ProxyHandler(Arc<ProxyHandlerInner>);

impl ProxyHandler {
    fn new(engine: wasmtime::Engine, instance_pre: InstancePre<Host>, args: Vec<String>, envs: Vec<(String, String)>) -> Self {
        Self(Arc::new(ProxyHandlerInner {
            engine,
            instance_pre,
            args,
            envs,
            next_id: AtomicU64::from(0),
        }))
    }
}

type Request = hyper::Request<hyper::body::Incoming>;

impl hyper::service::Service<Request> for ProxyHandler {
    type Response = hyper::Response<HyperOutgoingBody>;
    type Error = anyhow::Error;
    type Future = Pin<Box<dyn std::future::Future<Output = Result<Self::Response>> + Send>>;

    fn call(&self, req: Request) -> Self::Future {
        use http_body_util::BodyExt;

        let ProxyHandler(inner) = self.clone();

        let (sender, receiver) = tokio::sync::oneshot::channel();

        // TODO: need to track the join handle, but don't want to block the response on it
        tokio::task::spawn(async move {
            let req_id = inner.next_req_id();
            let (mut parts, body) = req.into_parts();

            parts.uri = {
                let uri_parts = parts.uri.into_parts();

                let scheme = uri_parts.scheme.unwrap_or(http::uri::Scheme::HTTP);

                let host = if let Some(val) = parts.headers.get(hyper::header::HOST) {
                    std::str::from_utf8(val.as_bytes())
                        .map_err(|_| http_types::ErrorCode::HttpRequestUriInvalid)?
                } else {
                    uri_parts
                        .authority
                        .as_ref()
                        .ok_or(http_types::ErrorCode::HttpRequestUriInvalid)?
                        .host()
                };

                let path_with_query = uri_parts
                    .path_and_query
                    .ok_or(http_types::ErrorCode::HttpRequestUriInvalid)?;

                hyper::Uri::builder()
                    .scheme(scheme)
                    .authority(host)
                    .path_and_query(path_with_query)
                    .build()
                    .map_err(|_| http_types::ErrorCode::HttpRequestUriInvalid)?
            };

            let req = hyper::Request::from_parts(parts, body.map_err(hyper_response_error).boxed());

            log::info!(
                "Request {req_id} handling {} to {}",
                req.method(),
                req.uri()
            );

            let mut store = new_store(&inner.engine, req_id, inner.args.clone(), inner.envs.clone())?;

            let req = store.data_mut().new_incoming_request(req)?;
            let out = store.data_mut().new_response_outparam(sender)?;

            let (proxy, _inst) =
                wasmtime_wasi_http::proxy::Proxy::instantiate_pre(&mut store, &inner.instance_pre)
                    .await?;

            if let Err(e) = proxy
                .wasi_http_incoming_handler()
                .call_handle(store, req, out)
                .await
            {
                log::error!("[{req_id}] :: {:#?}", e);
                return Err(e);
            }

            Ok(())
        });

        Box::pin(async move {
            match receiver.await {
                Ok(Ok(resp)) => Ok(resp),
                Ok(Err(e)) => Err(e.into()),
                Err(_) => bail!("guest never invoked `response-outparam::set` method"),
            }
        })
    }
}

fn new_store(engine: &wasmtime::Engine, req_id: u64, args: Vec<String>, envs: Vec<(String, String)>) -> Result<Store<Host>> {
    let file_perms = wasi_preview2::FilePerms::all();
    let dir_perms = wasi_preview2::DirPerms::all();

    let mut wasi_preview2_builder = wasi_preview2::WasiCtxBuilder::new();
    wasi_preview2_builder
        .args(args.as_slice())
        .envs(envs.as_slice())
        .inherit_stdio()
        .preopened_dir(
            Dir::from_std_file(File::open("/")?),
            dir_perms,
            file_perms,
            "/",
        );
    let wasi_preview2_ctx = wasi_preview2_builder.build();

    let mut host = Host {
        table: Table::new(),
        ctx: wasi_preview2_ctx,
        http: WasiHttpCtx,

        limits: StoreLimits::default(),
    };

    let mut store = Store::new(engine, host);

    store.limiter(|t| &mut t.limits);

    Ok(store)
}
