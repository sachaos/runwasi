use containerd_shim_wasm::sandbox::cli::{revision, shim_main, version};
use containerd_shim_wasmtimeserve::WasmtimeInstance;

fn main() {
    shim_main::<WasmtimeInstance>("wasmtimeserve", version!(), revision!(), "v1", None);
}
