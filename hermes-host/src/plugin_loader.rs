//! Dynamic Plugin Loader & ABI Validator
//!
//! Loads dynamic libraries (`.so`) using `libloading`, validates the Hermes C-ABI v1
//! descriptor, and provides safe wrappers around low-level FFI function calls.

use hermes_core::abi::{
    c_char_to_rust_str, c_char_to_rust_string, rust_str_to_c_char, AbiError,
    HermesPluginDescriptor, HERMES_ABI_VERSION, HERMES_ERR_BUFFER_TOO_SMALL, HERMES_ERR_INTERNAL,
    HERMES_ERR_INVALID_ARG, HERMES_ERR_INVALID_STATE, HERMES_ERR_NULL_PTR, HERMES_ERR_PANIC,
    HERMES_OK, HERMES_PLUGIN_ENTRY_SYMBOL, HERMES_PLUGIN_MAGIC,
};
use libloading::{Library, Symbol};
use std::ffi::c_void;
use std::os::raw::c_char;
use std::path::Path;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, info};

#[derive(Debug, Error)]
pub enum PluginLoaderError {
    #[error("Failed to load dynamic library {path}: {source}")]
    LibraryLoad {
        path: String,
        #[source]
        source: libloading::Error,
    },

    #[error("Failed to resolve symbol '{symbol}' in library {path}: {source}")]
    SymbolNotFound {
        path: String,
        symbol: String,
        #[source]
        source: libloading::Error,
    },

    #[error("Null descriptor returned from plugin entry point in {path}")]
    NullDescriptor { path: String },

    #[error("Invalid plugin magic: expected 0x{expected:X}, found 0x{found:X}")]
    InvalidMagic { expected: u32, found: u32 },

    #[error("Incompatible ABI version in {path}: expected {expected}, found {found}")]
    IncompatibleVersion {
        path: String,
        expected: u32,
        found: u32,
    },

    #[error("Plugin returned null function pointer in vtable for: {0}")]
    NullVTableFunction(&'static str),

    #[error("Plugin operation failed with code {0}: {1}")]
    ExecutionFailed(i32, String),

    #[error("Plugin panicked during invocation")]
    Panic,

    #[error("ABI error: {0}")]
    Abi(#[from] AbiError),

    #[error("UTF-8 decoding error across FFI boundary: {0}")]
    Utf8(#[from] std::str::Utf8Error),
}

/// A validated dynamic service library kept resident in memory.
#[derive(Clone)]
pub struct LoadedPlugin {
    path: String,
    // Keep library alive as long as LoadedPlugin or any instance exists
    _library: Arc<Library>,
    descriptor: HermesPluginDescriptor,
}

// Safety: HermesPluginDescriptor contains static read-only function pointers and string literals.
unsafe impl Send for LoadedPlugin {}
unsafe impl Sync for LoadedPlugin {}

impl LoadedPlugin {
    /// Loads a dynamic library from disk and validates its Hermes C-ABI compliance.
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, PluginLoaderError> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        debug!(path = %path_str, "Loading dynamic service library");

        let library = unsafe {
            Library::new(path.as_ref()).map_err(|e| PluginLoaderError::LibraryLoad {
                path: path_str.clone(),
                source: e,
            })?
        };

        // Resolve entry point
        type PluginEntryFn = unsafe extern "C" fn() -> *const HermesPluginDescriptor;
        let entry_fn: Symbol<PluginEntryFn> = unsafe {
            library
                .get(HERMES_PLUGIN_ENTRY_SYMBOL)
                .map_err(|e| PluginLoaderError::SymbolNotFound {
                    path: path_str.clone(),
                    symbol: "hermes_plugin_entry".to_string(),
                    source: e,
                })?
        };

        let descriptor_ptr = unsafe { entry_fn() };
        if descriptor_ptr.is_null() {
            return Err(PluginLoaderError::NullDescriptor { path: path_str });
        }

        let descriptor = unsafe { *descriptor_ptr };

        // 1. Verify Magic
        if descriptor.header.magic != HERMES_PLUGIN_MAGIC {
            return Err(PluginLoaderError::InvalidMagic {
                expected: HERMES_PLUGIN_MAGIC,
                found: descriptor.header.magic,
            });
        }

        // 2. Verify ABI Version
        if descriptor.header.abi_version != HERMES_ABI_VERSION {
            return Err(PluginLoaderError::IncompatibleVersion {
                path: path_str,
                expected: HERMES_ABI_VERSION,
                found: descriptor.header.abi_version,
            });
        }

        // 3. Verify Header String Pointers
        if descriptor.header.name.is_null()
            || descriptor.header.version.is_null()
            || descriptor.header.description.is_null()
        {
            return Err(PluginLoaderError::NullVTableFunction("header strings"));
        }

        let name = unsafe { c_char_to_rust_str(descriptor.header.name)? };
        let version = unsafe { c_char_to_rust_str(descriptor.header.version)? };

        info!(
            path = %path_str,
            plugin_name = %name,
            version = %version,
            "Successfully validated and loaded Hermes plugin"
        );

        Ok(Self {
            path: path_str,
            _library: Arc::new(library),
            descriptor,
        })
    }

    pub fn name(&self) -> &str {
        unsafe { c_char_to_rust_str(self.descriptor.header.name).unwrap_or("unknown") }
    }

    pub fn version(&self) -> &str {
        unsafe { c_char_to_rust_str(self.descriptor.header.version).unwrap_or("0.0.0") }
    }

    pub fn description(&self) -> &str {
        unsafe { c_char_to_rust_str(self.descriptor.header.description).unwrap_or("") }
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    /// Instantiates a new service instance via `create()`.
    pub fn create_instance(&self, config_json: &str) -> Result<*mut c_void, PluginLoaderError> {
        let mut instance: *mut c_void = std::ptr::null_mut();
        let config_c = rust_str_to_c_char(config_json);

        let code = unsafe { (self.descriptor.vtable.create)(config_c, &mut instance) };
        unsafe { hermes_core::abi::free_c_string(config_c) };

        self.check_return_code(code, "create")?;
        if instance.is_null() {
            return Err(PluginLoaderError::ExecutionFailed(
                code,
                "Plugin returned null instance pointer".to_string(),
            ));
        }

        Ok(instance)
    }

    /// Destroys a service instance via `destroy()`.
    pub fn destroy_instance(&self, instance: *mut c_void) -> Result<(), PluginLoaderError> {
        if instance.is_null() {
            return Ok(());
        }
        let code = unsafe { (self.descriptor.vtable.destroy)(instance) };
        self.check_return_code(code, "destroy")
    }

    /// Starts service execution via `start()`.
    pub fn start_instance(&self, instance: *mut c_void) -> Result<(), PluginLoaderError> {
        let code = unsafe { (self.descriptor.vtable.start)(instance) };
        self.check_return_code(code, "start")
    }

    /// Stops service execution via `stop()`.
    pub fn stop_instance(&self, instance: *mut c_void) -> Result<(), PluginLoaderError> {
        let code = unsafe { (self.descriptor.vtable.stop)(instance) };
        self.check_return_code(code, "stop")
    }

    /// Restarts service execution via `restart()`.
    pub fn restart_instance(&self, instance: *mut c_void) -> Result<(), PluginLoaderError> {
        let code = unsafe { (self.descriptor.vtable.restart)(instance) };
        self.check_return_code(code, "restart")
    }

    /// Retrieves operational status JSON string.
    pub fn get_status_json(&self, instance: *mut c_void) -> Result<String, PluginLoaderError> {
        self.call_json_getter(self.descriptor.vtable.get_status, instance, "get_status")
    }

    /// Retrieves health status JSON string.
    pub fn get_health_json(&self, instance: *mut c_void) -> Result<String, PluginLoaderError> {
        self.call_json_getter(self.descriptor.vtable.get_health, instance, "get_health")
    }

    /// Retrieves deep diagnostics JSON string.
    pub fn get_diagnostics_json(&self, instance: *mut c_void) -> Result<String, PluginLoaderError> {
        self.call_json_getter(
            self.descriptor.vtable.get_diagnostics,
            instance,
            "get_diagnostics",
        )
    }

    /// Retrieves thread call stacks JSON string.
    pub fn get_thread_stacks_json(
        &self,
        instance: *mut c_void,
    ) -> Result<String, PluginLoaderError> {
        self.call_json_getter(
            self.descriptor.vtable.get_all_thread_call_stacks,
            instance,
            "get_all_thread_call_stacks",
        )
    }

    /// Retrieves runtime configuration JSON string.
    pub fn get_configuration_json(
        &self,
        instance: *mut c_void,
    ) -> Result<String, PluginLoaderError> {
        self.call_json_getter(
            self.descriptor.vtable.get_configuration,
            instance,
            "get_configuration",
        )
    }

    fn call_json_getter(
        &self,
        getter: unsafe extern "C" fn(*mut c_void, *mut *mut c_char) -> i32,
        instance: *mut c_void,
        op_name: &'static str,
    ) -> Result<String, PluginLoaderError> {
        let mut out_ptr: *mut c_char = std::ptr::null_mut();
        let code = unsafe { getter(instance, &mut out_ptr) };
        self.check_return_code(code, op_name)?;

        if out_ptr.is_null() {
            return Ok("{}".to_string());
        }

        let result = unsafe { c_char_to_rust_string(out_ptr) }
            .map_err(|e| PluginLoaderError::ExecutionFailed(code, e.to_string()));

        // Always free memory using plugin's own free_string export
        unsafe { (self.descriptor.vtable.free_string)(out_ptr) };

        result
    }

    fn check_return_code(&self, code: i32, operation: &'static str) -> Result<(), PluginLoaderError> {
        match code {
            HERMES_OK => Ok(()),
            HERMES_ERR_PANIC => Err(PluginLoaderError::Panic),
            HERMES_ERR_INVALID_ARG => Err(PluginLoaderError::ExecutionFailed(
                code,
                format!("{}: Invalid argument passed", operation),
            )),
            HERMES_ERR_NULL_PTR => Err(PluginLoaderError::ExecutionFailed(
                code,
                format!("{}: Null pointer passed across FFI", operation),
            )),
            HERMES_ERR_INVALID_STATE => Err(PluginLoaderError::ExecutionFailed(
                code,
                format!("{}: Illegal operation in current service state", operation),
            )),
            HERMES_ERR_BUFFER_TOO_SMALL => Err(PluginLoaderError::ExecutionFailed(
                code,
                format!("{}: Buffer too small", operation),
            )),
            HERMES_ERR_INTERNAL | _ => Err(PluginLoaderError::ExecutionFailed(
                code,
                format!("{}: Internal plugin error (code {})", operation, code),
            )),
        }
    }
}
