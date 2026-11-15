//! C-Compatible Dynamic Plugin ABI Specification (`hermes_abi_v1`)
//!
//! Provides ABI-stable FFI definitions, return codes, function vtables,
//! safe memory management utilities, and adapter macros for dynamically linked plugins.

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_void};
use thiserror::Error;

/// Magic bytes identifying a valid Hermes plugin library ("HERM" in ASCII little-endian).
pub const HERMES_PLUGIN_MAGIC: u32 = 0x4845524D;

/// Current ABI version. Incremented on breaking FFI changes.
pub const HERMES_ABI_VERSION: u32 = 1;

/// Standard entry point symbol name exported by all Hermes plugins.
pub const HERMES_PLUGIN_ENTRY_SYMBOL: &[u8] = b"hermes_plugin_entry\0";

// Standard ABI Return Codes
pub const HERMES_OK: i32 = 0;
pub const HERMES_ERR_INVALID_ARG: i32 = -1;
pub const HERMES_ERR_NULL_PTR: i32 = -2;
pub const HERMES_ERR_INTERNAL: i32 = -3;
pub const HERMES_ERR_INVALID_STATE: i32 = -4;
pub const HERMES_ERR_PANIC: i32 = -5;
pub const HERMES_ERR_BUFFER_TOO_SMALL: i32 = -6;

/// Metadata header describing the plugin and verifying ABI compatibility.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct HermesPluginHeader {
    /// Must match `HERMES_PLUGIN_MAGIC`.
    pub magic: u32,
    /// Must match `HERMES_ABI_VERSION`.
    pub abi_version: u32,
    /// Null-terminated UTF-8 plugin name.
    pub name: *const c_char,
    /// Null-terminated UTF-8 plugin semantic version.
    pub version: *const c_char,
    /// Null-terminated UTF-8 description.
    pub description: *const c_char,
    /// Null-terminated UTF-8 author/maintainer string.
    pub author: *const c_char,
}

/// Function table defining the plugin lifecycle and diagnostic contract.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct HermesPluginVTable {
    /// Instantiates a new service instance using a UTF-8 JSON configuration string.
    /// Stores the opaque instance pointer in `out_instance`.
    pub create: unsafe extern "C" fn(
        config_json: *const c_char,
        out_instance: *mut *mut c_void,
    ) -> i32,

    /// Releases the service instance and frees all associated host resources.
    pub destroy: unsafe extern "C" fn(instance: *mut c_void) -> i32,

    /// Starts service execution and background worker threads.
    pub start: unsafe extern "C" fn(instance: *mut c_void) -> i32,

    /// Initiates graceful, cooperative shutdown of worker threads.
    pub stop: unsafe extern "C" fn(instance: *mut c_void) -> i32,

    /// Stops and restarts the service instance.
    pub restart: unsafe extern "C" fn(instance: *mut c_void) -> i32,

    /// Retrieves current operational status serialized as UTF-8 JSON.
    /// The returned string must be freed by calling `free_string`.
    pub get_status: unsafe extern "C" fn(
        instance: *mut c_void,
        out_json: *mut *mut c_char,
    ) -> i32,

    /// Retrieves health metrics and status flags serialized as UTF-8 JSON.
    /// The returned string must be freed by calling `free_string`.
    pub get_health: unsafe extern "C" fn(
        instance: *mut c_void,
        out_json: *mut *mut c_char,
    ) -> i32,

    /// Retrieves deep diagnostic data (counters, internal state, queues) as UTF-8 JSON.
    /// The returned string must be freed by calling `free_string`.
    pub get_diagnostics: unsafe extern "C" fn(
        instance: *mut c_void,
        out_json: *mut *mut c_char,
    ) -> i32,

    /// Requests call stacks across all active threads registered with the service as UTF-8 JSON.
    /// The returned string must be freed by calling `free_string`.
    pub get_all_thread_call_stacks: unsafe extern "C" fn(
        instance: *mut c_void,
        out_json: *mut *mut c_char,
    ) -> i32,

    /// Returns the effective runtime configuration serialized as UTF-8 JSON.
    /// The returned string must be freed by calling `free_string`.
    pub get_configuration: unsafe extern "C" fn(
        instance: *mut c_void,
        out_json: *mut *mut c_char,
    ) -> i32,

    /// Deallocates a string buffer previously returned by any vtable function.
    pub free_string: unsafe extern "C" fn(ptr: *mut c_char),
}

/// The complete plugin descriptor exported by dynamic libraries.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct HermesPluginDescriptor {
    pub header: HermesPluginHeader,
    pub vtable: HermesPluginVTable,
}

// Safety: The descriptor, header, and vtable point to static read-only vtables and string literals.
unsafe impl Sync for HermesPluginHeader {}
unsafe impl Send for HermesPluginHeader {}
unsafe impl Sync for HermesPluginVTable {}
unsafe impl Send for HermesPluginVTable {}
unsafe impl Sync for HermesPluginDescriptor {}
unsafe impl Send for HermesPluginDescriptor {}

/// High-level, safe Rust trait implemented by service plugin authors.
/// The container uses this trait via the C-ABI adapter macros.
pub trait HermesServicePlugin: Send + Sync + 'static {
    fn new(config_json: &str) -> Result<Self, String>
    where
        Self: Sized;

    fn start(&mut self) -> Result<(), String>;
    fn stop(&mut self) -> Result<(), String>;
    fn restart(&mut self) -> Result<(), String>;

    fn get_status_json(&self) -> Result<String, String>;
    fn get_health_json(&self) -> Result<String, String>;
    fn get_diagnostics_json(&self) -> Result<String, String>;
    fn get_all_thread_call_stacks_json(&self) -> Result<String, String>;
    fn get_configuration_json(&self) -> Result<String, String>;
}

/// Errors occurring at the FFI / ABI layer.
#[derive(Debug, Error)]
pub enum AbiError {
    #[error("Null pointer provided to FFI function: {0}")]
    NullPointer(&'static str),

    #[error("Invalid UTF-8 string across FFI boundary: {0}")]
    Utf8Error(#[from] std::str::Utf8Error),

    #[error("Plugin returned error code {0}")]
    PluginError(i32),

    #[error("Uncaught panic in plugin call: {0}")]
    Panic(String),

    #[error("Invalid magic number: expected 0x{expected:X}, found 0x{found:X}")]
    InvalidMagic { expected: u32, found: u32 },

    #[error("Incompatible ABI version: expected {expected}, found {found}")]
    IncompatibleVersion { expected: u32, found: u32 },
}

/// Allocates an owned C string on the heap from a Rust string slice.
/// Must be freed using `free_c_string` from the same allocator.
pub fn rust_str_to_c_char(s: &str) -> *mut c_char {
    match CString::new(s) {
        Ok(c_str) => c_str.into_raw(),
        Err(_) => {
            // If string contains internal null byte, sanitize it
            let sanitized = s.replace('\0', " ");
            CString::new(sanitized)
                .unwrap_or_else(|_| CString::new("").unwrap())
                .into_raw()
        }
    }
}

/// Converts a raw C-string pointer to a safe Rust &str.
///
/// # Safety
/// The pointer must be non-null and point to a valid null-terminated C string.
pub unsafe fn c_char_to_rust_str<'a>(ptr: *const c_char) -> Result<&'a str, AbiError> {
    if ptr.is_null() {
        return Err(AbiError::NullPointer("c_char_to_rust_str"));
    }
    let c_str = CStr::from_ptr(ptr);
    c_str.to_str().map_err(AbiError::Utf8Error)
}

/// Converts a raw C-string pointer into an owned Rust `String`.
///
/// # Safety
/// The pointer must be non-null and point to a valid null-terminated C string.
pub unsafe fn c_char_to_rust_string(ptr: *const c_char) -> Result<String, AbiError> {
    c_char_to_rust_str(ptr).map(|s| s.to_string())
}

/// Deallocates a C string previously created with `rust_str_to_c_char`.
///
/// # Safety
/// `ptr` must be a pointer returned from `CString::into_raw` in the same allocator.
pub unsafe fn free_c_string(ptr: *mut c_char) {
    if !ptr.is_null() {
        drop(CString::from_raw(ptr));
    }
}

/// Macro for service authors to export a complete, safe Hermes C-ABI plugin.
#[macro_export]
macro_rules! declare_hermes_plugin {
    (
        type: $plugin_type:ty,
        name: $name:expr,
        version: $version:expr,
        description: $desc:expr,
        author: $author:expr
    ) => {
        static PLUGIN_NAME: &[u8] = concat!($name, "\0").as_bytes();
        static PLUGIN_VERSION: &[u8] = concat!($version, "\0").as_bytes();
        static PLUGIN_DESC: &[u8] = concat!($desc, "\0").as_bytes();
        static PLUGIN_AUTHOR: &[u8] = concat!($author, "\0").as_bytes();

        static PLUGIN_HEADER: $crate::abi::HermesPluginHeader = $crate::abi::HermesPluginHeader {
            magic: $crate::abi::HERMES_PLUGIN_MAGIC,
            abi_version: $crate::abi::HERMES_ABI_VERSION,
            name: PLUGIN_NAME.as_ptr() as *const ::std::os::raw::c_char,
            version: PLUGIN_VERSION.as_ptr() as *const ::std::os::raw::c_char,
            description: PLUGIN_DESC.as_ptr() as *const ::std::os::raw::c_char,
            author: PLUGIN_AUTHOR.as_ptr() as *const ::std::os::raw::c_char,
        };

        unsafe extern "C" fn __hermes_create(
            config_json: *const ::std::os::raw::c_char,
            out_instance: *mut *mut ::std::os::raw::c_void,
        ) -> i32 {
            if out_instance.is_null() {
                return $crate::abi::HERMES_ERR_NULL_PTR;
            }
            let res = ::std::panic::catch_unwind(|| {
                let config_str = if config_json.is_null() {
                    "{}"
                } else {
                    match $crate::abi::c_char_to_rust_str(config_json) {
                        Ok(s) => s,
                        Err(_) => return $crate::abi::HERMES_ERR_INVALID_ARG,
                    }
                };

                match <$plugin_type as $crate::abi::HermesServicePlugin>::new(config_str) {
                    Ok(instance) => {
                        let boxed = Box::new(instance);
                        *out_instance = Box::into_raw(boxed) as *mut ::std::os::raw::c_void;
                        $crate::abi::HERMES_OK
                    }
                    Err(_) => $crate::abi::HERMES_ERR_INTERNAL,
                }
            });

            match res {
                Ok(code) => code,
                Err(_) => $crate::abi::HERMES_ERR_PANIC,
            }
        }

        unsafe extern "C" fn __hermes_destroy(instance: *mut ::std::os::raw::c_void) -> i32 {
            if instance.is_null() {
                return $crate::abi::HERMES_ERR_NULL_PTR;
            }
            let res = ::std::panic::catch_unwind(|| {
                let boxed = Box::from_raw(instance as *mut $plugin_type);
                drop(boxed);
                $crate::abi::HERMES_OK
            });
            match res {
                Ok(code) => code,
                Err(_) => $crate::abi::HERMES_ERR_PANIC,
            }
        }

        unsafe extern "C" fn __hermes_start(instance: *mut ::std::os::raw::c_void) -> i32 {
            if instance.is_null() {
                return $crate::abi::HERMES_ERR_NULL_PTR;
            }
            let res = ::std::panic::catch_unwind(|| {
                let typed = &mut *(instance as *mut $plugin_type);
                match typed.start() {
                    Ok(()) => $crate::abi::HERMES_OK,
                    Err(_) => $crate::abi::HERMES_ERR_INTERNAL,
                }
            });
            match res {
                Ok(code) => code,
                Err(_) => $crate::abi::HERMES_ERR_PANIC,
            }
        }

        unsafe extern "C" fn __hermes_stop(instance: *mut ::std::os::raw::c_void) -> i32 {
            if instance.is_null() {
                return $crate::abi::HERMES_ERR_NULL_PTR;
            }
            let res = ::std::panic::catch_unwind(|| {
                let typed = &mut *(instance as *mut $plugin_type);
                match typed.stop() {
                    Ok(()) => $crate::abi::HERMES_OK,
                    Err(_) => $crate::abi::HERMES_ERR_INTERNAL,
                }
            });
            match res {
                Ok(code) => code,
                Err(_) => $crate::abi::HERMES_ERR_PANIC,
            }
        }

        unsafe extern "C" fn __hermes_restart(instance: *mut ::std::os::raw::c_void) -> i32 {
            if instance.is_null() {
                return $crate::abi::HERMES_ERR_NULL_PTR;
            }
            let res = ::std::panic::catch_unwind(|| {
                let typed = &mut *(instance as *mut $plugin_type);
                match typed.restart() {
                    Ok(()) => $crate::abi::HERMES_OK,
                    Err(_) => $crate::abi::HERMES_ERR_INTERNAL,
                }
            });
            match res {
                Ok(code) => code,
                Err(_) => $crate::abi::HERMES_ERR_PANIC,
            }
        }

        unsafe extern "C" fn __hermes_get_status(
            instance: *mut ::std::os::raw::c_void,
            out_json: *mut *mut ::std::os::raw::c_char,
        ) -> i32 {
            if instance.is_null() || out_json.is_null() {
                return $crate::abi::HERMES_ERR_NULL_PTR;
            }
            let res = ::std::panic::catch_unwind(|| {
                let typed = &*(instance as *const $plugin_type);
                match typed.get_status_json() {
                    Ok(json) => {
                        *out_json = $crate::abi::rust_str_to_c_char(&json);
                        $crate::abi::HERMES_OK
                    }
                    Err(_) => $crate::abi::HERMES_ERR_INTERNAL,
                }
            });
            match res {
                Ok(code) => code,
                Err(_) => $crate::abi::HERMES_ERR_PANIC,
            }
        }

        unsafe extern "C" fn __hermes_get_health(
            instance: *mut ::std::os::raw::c_void,
            out_json: *mut *mut ::std::os::raw::c_char,
        ) -> i32 {
            if instance.is_null() || out_json.is_null() {
                return $crate::abi::HERMES_ERR_NULL_PTR;
            }
            let res = ::std::panic::catch_unwind(|| {
                let typed = &*(instance as *const $plugin_type);
                match typed.get_health_json() {
                    Ok(json) => {
                        *out_json = $crate::abi::rust_str_to_c_char(&json);
                        $crate::abi::HERMES_OK
                    }
                    Err(_) => $crate::abi::HERMES_ERR_INTERNAL,
                }
            });
            match res {
                Ok(code) => code,
                Err(_) => $crate::abi::HERMES_ERR_PANIC,
            }
        }

        unsafe extern "C" fn __hermes_get_diagnostics(
            instance: *mut ::std::os::raw::c_void,
            out_json: *mut *mut ::std::os::raw::c_char,
        ) -> i32 {
            if instance.is_null() || out_json.is_null() {
                return $crate::abi::HERMES_ERR_NULL_PTR;
            }
            let res = ::std::panic::catch_unwind(|| {
                let typed = &*(instance as *const $plugin_type);
                match typed.get_diagnostics_json() {
                    Ok(json) => {
                        *out_json = $crate::abi::rust_str_to_c_char(&json);
                        $crate::abi::HERMES_OK
                    }
                    Err(_) => $crate::abi::HERMES_ERR_INTERNAL,
                }
            });
            match res {
                Ok(code) => code,
                Err(_) => $crate::abi::HERMES_ERR_PANIC,
            }
        }

        unsafe extern "C" fn __hermes_get_all_thread_call_stacks(
            instance: *mut ::std::os::raw::c_void,
            out_json: *mut *mut ::std::os::raw::c_char,
        ) -> i32 {
            if instance.is_null() || out_json.is_null() {
                return $crate::abi::HERMES_ERR_NULL_PTR;
            }
            let res = ::std::panic::catch_unwind(|| {
                let typed = &*(instance as *const $plugin_type);
                match typed.get_all_thread_call_stacks_json() {
                    Ok(json) => {
                        *out_json = $crate::abi::rust_str_to_c_char(&json);
                        $crate::abi::HERMES_OK
                    }
                    Err(_) => $crate::abi::HERMES_ERR_INTERNAL,
                }
            });
            match res {
                Ok(code) => code,
                Err(_) => $crate::abi::HERMES_ERR_PANIC,
            }
        }

        unsafe extern "C" fn __hermes_get_configuration(
            instance: *mut ::std::os::raw::c_void,
            out_json: *mut *mut ::std::os::raw::c_char,
        ) -> i32 {
            if instance.is_null() || out_json.is_null() {
                return $crate::abi::HERMES_ERR_NULL_PTR;
            }
            let res = ::std::panic::catch_unwind(|| {
                let typed = &*(instance as *const $plugin_type);
                match typed.get_configuration_json() {
                    Ok(json) => {
                        *out_json = $crate::abi::rust_str_to_c_char(&json);
                        $crate::abi::HERMES_OK
                    }
                    Err(_) => $crate::abi::HERMES_ERR_INTERNAL,
                }
            });
            match res {
                Ok(code) => code,
                Err(_) => $crate::abi::HERMES_ERR_PANIC,
            }
        }

        unsafe extern "C" fn __hermes_free_string(ptr: *mut ::std::os::raw::c_char) {
            $crate::abi::free_c_string(ptr);
        }

        static PLUGIN_VTABLE: $crate::abi::HermesPluginVTable = $crate::abi::HermesPluginVTable {
            create: __hermes_create,
            destroy: __hermes_destroy,
            start: __hermes_start,
            stop: __hermes_stop,
            restart: __hermes_restart,
            get_status: __hermes_get_status,
            get_health: __hermes_get_health,
            get_diagnostics: __hermes_get_diagnostics,
            get_all_thread_call_stacks: __hermes_get_all_thread_call_stacks,
            get_configuration: __hermes_get_configuration,
            free_string: __hermes_free_string,
        };

        static PLUGIN_DESCRIPTOR: $crate::abi::HermesPluginDescriptor =
            $crate::abi::HermesPluginDescriptor {
                header: PLUGIN_HEADER,
                vtable: PLUGIN_VTABLE,
            };

        #[no_mangle]
        pub extern "C" fn hermes_plugin_entry() -> *const $crate::abi::HermesPluginDescriptor {
            &PLUGIN_DESCRIPTOR as *const _
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DummyService {
        running: bool,
    }

    impl HermesServicePlugin for DummyService {
        fn new(_config_json: &str) -> Result<Self, String> {
            Ok(DummyService { running: false })
        }

        fn start(&mut self) -> Result<(), String> {
            self.running = true;
            Ok(())
        }

        fn stop(&mut self) -> Result<(), String> {
            self.running = false;
            Ok(())
        }

        fn restart(&mut self) -> Result<(), String> {
            self.stop()?;
            self.start()?;
            Ok(())
        }

        fn get_status_json(&self) -> Result<String, String> {
            Ok(format!(r#"{{"running":{}}}"#, self.running))
        }

        fn get_health_json(&self) -> Result<String, String> {
            Ok(r#"{"healthy":true}"#.to_string())
        }

        fn get_diagnostics_json(&self) -> Result<String, String> {
            Ok(r#"{"metric":42}"#.to_string())
        }

        fn get_all_thread_call_stacks_json(&self) -> Result<String, String> {
            Ok(r#"[]"#.to_string())
        }

        fn get_configuration_json(&self) -> Result<String, String> {
            Ok(r#"{}"#.to_string())
        }
    }

    // Instantiate plugin using the macro
    declare_hermes_plugin!(
        type: DummyService,
        name: "dummy-service",
        version: "1.0.0",
        description: "Test service plugin",
        author: "Hermes Core Test"
    );

    #[test]
    fn test_abi_descriptor_entry_point() {
        let descriptor_ptr = hermes_plugin_entry();
        assert!(!descriptor_ptr.is_null());

        unsafe {
            let desc = &*descriptor_ptr;
            assert_eq!(desc.header.magic, HERMES_PLUGIN_MAGIC);
            assert_eq!(desc.header.abi_version, HERMES_ABI_VERSION);

            let name = c_char_to_rust_str(desc.header.name).unwrap();
            assert_eq!(name, "dummy-service");

            let ver = c_char_to_rust_str(desc.header.version).unwrap();
            assert_eq!(ver, "1.0.0");
        }
    }

    #[test]
    fn test_plugin_vtable_lifecycle() {
        unsafe {
            let desc = &*hermes_plugin_entry();
            let mut instance: *mut c_void = std::ptr::null_mut();

            // Create instance
            let config_c = rust_str_to_c_char(r#"{"sample":true}"#);
            let code = (desc.vtable.create)(config_c, &mut instance);
            free_c_string(config_c);
            assert_eq!(code, HERMES_OK);
            assert!(!instance.is_null());

            // Check initial status
            let mut status_ptr: *mut c_char = std::ptr::null_mut();
            let code = (desc.vtable.get_status)(instance, &mut status_ptr);
            assert_eq!(code, HERMES_OK);
            let status = c_char_to_rust_string(status_ptr).unwrap();
            assert!(status.contains(r#""running":false"#));
            (desc.vtable.free_string)(status_ptr);

            // Start instance
            let code = (desc.vtable.start)(instance);
            assert_eq!(code, HERMES_OK);

            // Check running status
            let mut status_ptr2: *mut c_char = std::ptr::null_mut();
            let code = (desc.vtable.get_status)(instance, &mut status_ptr2);
            assert_eq!(code, HERMES_OK);
            let status2 = c_char_to_rust_string(status_ptr2).unwrap();
            assert!(status2.contains(r#""running":true"#));
            (desc.vtable.free_string)(status_ptr2);

            // Stop instance
            let code = (desc.vtable.stop)(instance);
            assert_eq!(code, HERMES_OK);

            // Destroy instance
            let code = (desc.vtable.destroy)(instance);
            assert_eq!(code, HERMES_OK);
        }
    }
}
