use std::{
    ffi::{OsStr, OsString, c_void},
    io,
    mem::{size_of, zeroed},
    os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle},
    ptr,
    time::Duration,
};
use tokio::net::windows::named_pipe::{ClientOptions, NamedPipeServer, ServerOptions};
use windows_sys::Win32::{
    Foundation::{ERROR_PIPE_BUSY, LocalFree},
    Security::{Authorization::*, *},
    System::Threading::{GetCurrentProcess, OpenProcessToken},
};

pub struct Listener {
    name: OsString,
    pipe: NamedPipeServer,
    security: SecurityDescriptor,
}

impl Listener {
    pub async fn bind(endpoint: &OsStr) -> io::Result<Self> {
        validate_name(endpoint)?;
        let security = SecurityDescriptor::current_user()?;
        let pipe = create(endpoint, &security, true)?;
        Ok(Self {
            name: endpoint.to_owned(),
            pipe,
            security,
        })
    }

    pub async fn accept(&mut self) -> io::Result<super::Stream> {
        self.pipe.connect().await?;
        // Install the next instance before handing this one to a request handler.
        let next = create(&self.name, &self.security, false)?;
        Ok(Box::new(std::mem::replace(&mut self.pipe, next)))
    }
}

fn create(name: &OsStr, security: &SecurityDescriptor, first: bool) -> io::Result<NamedPipeServer> {
    let mut attributes = SECURITY_ATTRIBUTES {
        nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: security.0.0,
        bInheritHandle: 0,
    };
    // Tokio passes this descriptor synchronously to CreateNamedPipeW; it remains owned here.
    unsafe {
        ServerOptions::new()
            .first_pipe_instance(first)
            .reject_remote_clients(true)
            .create_with_security_attributes_raw(
                name,
                (&mut attributes as *mut SECURITY_ATTRIBUTES).cast(),
            )
    }
}

pub async fn connect(endpoint: &OsStr) -> io::Result<super::Stream> {
    validate_name(endpoint)?;
    loop {
        match ClientOptions::new().open(endpoint) {
            Ok(pipe) => return Ok(Box::new(pipe)),
            Err(e) if e.raw_os_error() == Some(ERROR_PIPE_BUSY as i32) => {
                tokio::time::sleep(Duration::from_millis(25)).await
            }
            Err(e) => return Err(e),
        }
    }
}

fn validate_name(endpoint: &OsStr) -> io::Result<()> {
    let name = endpoint.to_string_lossy();
    if !name.to_ascii_lowercase().starts_with(r"\\.\pipe\") || name.len() <= 9 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            r"Windows endpoints must use \\.\pipe\NAME",
        ));
    }
    Ok(())
}

struct LocalAllocation(*mut c_void);
impl Drop for LocalAllocation {
    fn drop(&mut self) {
        unsafe {
            LocalFree(self.0);
        }
    }
}

struct SecurityDescriptor(LocalAllocation);
// The allocated descriptor is immutable and only freed after all create calls finish.
unsafe impl Send for SecurityDescriptor {}
unsafe impl Sync for SecurityDescriptor {}

impl SecurityDescriptor {
    fn current_user() -> io::Result<Self> {
        let mut token = ptr::null_mut();
        check(unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) })?;
        let token = unsafe { OwnedHandle::from_raw_handle(token) };
        let mut bytes = 0;
        unsafe {
            GetTokenInformation(
                token.as_raw_handle(),
                TokenUser,
                ptr::null_mut(),
                0,
                &mut bytes,
            );
        }
        let mut buffer = vec![0usize; (bytes as usize).div_ceil(size_of::<usize>())];
        check(unsafe {
            GetTokenInformation(
                token.as_raw_handle(),
                TokenUser,
                buffer.as_mut_ptr().cast(),
                bytes,
                &mut bytes,
            )
        })?;
        let user = unsafe { &*buffer.as_ptr().cast::<TOKEN_USER>() };
        let mut sid = ptr::null_mut();
        check(unsafe { ConvertSidToStringSidW(user.User.Sid, &mut sid) })?;
        let sid_memory = LocalAllocation(sid.cast());
        let mut length = 0;
        while unsafe { *sid.add(length) } != 0 {
            length += 1;
        }
        let sid_text = String::from_utf16_lossy(unsafe { std::slice::from_raw_parts(sid, length) });
        drop(sid_memory);
        let sddl: Vec<_> = format!("D:P(A;;GA;;;{sid_text})")
            .encode_utf16()
            .chain([0])
            .collect();
        let mut descriptor = unsafe { zeroed() };
        check(unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                sddl.as_ptr(),
                SDDL_REVISION_1,
                &mut descriptor,
                ptr::null_mut(),
            )
        })?;
        Ok(Self(LocalAllocation(descriptor)))
    }
}

fn check(success: i32) -> io::Result<()> {
    if success == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}
