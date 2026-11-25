//! Memory data from macOS. On other systems each function returns `None`.

/// The memory that the system can give without swap, in MB:
/// free + inactive + purgeable pages.
pub fn available_mb() -> Option<u64> {
    imp::available_mb()
}

/// `kern.memorystatus_vm_pressure_level`: 1 = normal, 2 = warning, 4 = critical.
pub fn pressure_level() -> Option<u32> {
    imp::pressure_level()
}

/// The used swap space in MB (`vm.swapusage`).
pub fn swap_used_mb() -> Option<u64> {
    imp::swap_used_mb()
}

/// The `phys_footprint` of a process in bytes. This is the value that the
/// `footprint` tool shows. RSS is not correct for engines that use the GPU.
pub fn phys_footprint(pid: i32) -> Option<u64> {
    imp::phys_footprint(pid)
}

#[cfg(target_os = "macos")]
mod imp {
    use std::ffi::CString;
    use std::mem::{size_of, zeroed};

    fn sysctl<T: Copy>(name: &str) -> Option<T> {
        let name = CString::new(name).ok()?;
        let mut len = size_of::<T>();
        // SAFETY: `value` is a plain C struct of `len` bytes, and sysctlbyname
        // writes at most `len` bytes into it.
        unsafe {
            let mut value: T = zeroed();
            let rc = libc::sysctlbyname(name.as_ptr(), (&mut value as *mut T).cast(), &mut len, std::ptr::null_mut(), 0);
            (rc == 0 && len == size_of::<T>()).then_some(value)
        }
    }

    #[allow(deprecated)] // libc points to the mach2 crate; one call does not need a new dependency.
    pub fn available_mb() -> Option<u64> {
        // SAFETY: `stats` has the size that `count` gives, in units of integer_t.
        unsafe {
            let mut stats: libc::vm_statistics64 = zeroed();
            let mut count = (size_of::<libc::vm_statistics64>() / size_of::<libc::integer_t>()) as libc::mach_msg_type_number_t;
            let rc = libc::host_statistics64(libc::mach_host_self(), libc::HOST_VM_INFO64, (&mut stats as *mut libc::vm_statistics64).cast(), &mut count);
            if rc != 0 {
                return None;
            }
            let pages = stats.free_count as u64 + stats.inactive_count as u64 + stats.purgeable_count as u64;
            Some(pages * libc::sysconf(libc::_SC_PAGESIZE) as u64 / (1024 * 1024))
        }
    }

    pub fn pressure_level() -> Option<u32> {
        sysctl::<u32>("kern.memorystatus_vm_pressure_level")
    }

    pub fn swap_used_mb() -> Option<u64> {
        sysctl::<libc::xsw_usage>("vm.swapusage").map(|s| s.xsu_used / (1024 * 1024))
    }

    pub fn phys_footprint(pid: i32) -> Option<u64> {
        // SAFETY: `info` is the struct that RUSAGE_INFO_V4 fills.
        unsafe {
            let mut info: libc::rusage_info_v4 = zeroed();
            let rc = libc::proc_pid_rusage(pid, libc::RUSAGE_INFO_V4, (&mut info as *mut libc::rusage_info_v4).cast());
            (rc == 0).then_some(info.ri_phys_footprint)
        }
    }
}

#[cfg(not(target_os = "macos"))]
mod imp {
    pub fn available_mb() -> Option<u64> {
        None
    }
    pub fn pressure_level() -> Option<u32> {
        None
    }
    pub fn swap_used_mb() -> Option<u64> {
        None
    }
    pub fn phys_footprint(_pid: i32) -> Option<u64> {
        None
    }
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::*;

    #[test]
    fn macos_gives_memory_data() {
        assert!(available_mb().unwrap() > 0);
        assert!(matches!(pressure_level(), Some(1 | 2 | 4)));
        assert!(swap_used_mb().is_some());
        let me = phys_footprint(std::process::id() as i32).unwrap();
        assert!(me > 100_000, "footprint {me}");
    }
}
