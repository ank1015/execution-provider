//! Windows children enter their Job Object as part of CreateProcess, before any
//! user code can run. Pipe handles are inherited through an explicit handle list.
use super::super::{Control, Launch};
use crate::{Error, ErrorCode, IoMode, OutputStream, Result};
use std::{
    ffi::{OsStr, c_void},
    fs::File,
    io,
    mem::{size_of, size_of_val, zeroed},
    os::windows::{
        ffi::OsStrExt,
        io::{AsRawHandle, FromRawHandle, OwnedHandle},
    },
    ptr,
    sync::Arc,
};
use windows_sys::Win32::{
    Foundation::*,
    Security::SECURITY_ATTRIBUTES,
    System::{Console::*, JobObjects::*, Pipes::CreatePipe, Threading::*},
};

pub(super) struct NativeProcess {
    pub control: Arc<dyn Control>,
    pub child: OwnedHandle,
    pub readers: Vec<(OutputStream, File)>,
    pub writer: Option<File>,
}

struct WindowsControl {
    job: OwnedHandle,
    terminal: Option<Console>,
}

impl Control for WindowsControl {
    fn interrupt(&self) -> Result<()> {
        Err(Error::new(
            ErrorCode::UnsupportedOperation,
            "piped Windows processes have no portable interrupt operation",
        ))
    }
    fn terminate(&self) -> Result<bool> {
        Ok(false)
    }
    fn kill(&self) -> Result<()> {
        check(unsafe { TerminateJobObject(self.job.as_raw_handle(), 1) })
    }
    fn resize(&self, rows: u16, cols: u16) -> Result<()> {
        let console = self.terminal.as_ref().ok_or_else(|| {
            Error::new(ErrorCode::UnsupportedOperation, "execution has no terminal")
        })?;
        hresult(unsafe {
            ResizePseudoConsole(
                console.handle,
                COORD {
                    X: cols as i16,
                    Y: rows as i16,
                },
            )
        })
    }
}

struct Console {
    handle: HPCON,
    _input: File,
    _output: File,
}
impl Drop for Console {
    fn drop(&mut self) {
        unsafe {
            ClosePseudoConsole(self.handle);
        }
    }
}

pub(super) fn spawn(launch: &Launch) -> Result<NativeProcess> {
    let job = create_job()?;
    let (input_read, input_write) = pipe()?;
    let (output_read, output_write) = pipe()?;
    let job_handle = job.as_raw_handle();
    let mut startup: STARTUPINFOEXW = unsafe { zeroed() };
    startup.StartupInfo.cb = size_of::<STARTUPINFOEXW>() as u32;

    let mut terminal = None;
    let mut stderr = None;
    let mut inherited = Vec::new();
    match launch.io {
        IoMode::Pty { rows, cols } => {
            // Use the pseudoconsole's standard handles, even when the supervisor's
            // own stdin/stdout/stderr are redirected to null, pipes, or log files.
            startup.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
            startup.StartupInfo.hStdInput = INVALID_HANDLE_VALUE;
            startup.StartupInfo.hStdOutput = INVALID_HANDLE_VALUE;
            startup.StartupInfo.hStdError = INVALID_HANDLE_VALUE;
            let mut handle = 0;
            hresult(unsafe {
                CreatePseudoConsole(
                    COORD {
                        X: cols as i16,
                        Y: rows as i16,
                    },
                    input_read.as_raw_handle(),
                    output_write.as_raw_handle(),
                    0,
                    &mut handle,
                )
            })?;
            terminal = Some(Console {
                handle,
                _input: input_read,
                _output: output_write,
            });
        }
        IoMode::Pipes { .. } => {
            let (read, write) = pipe()?;
            startup.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
            startup.StartupInfo.hStdInput = input_read.as_raw_handle();
            startup.StartupInfo.hStdOutput = output_write.as_raw_handle();
            startup.StartupInfo.hStdError = write.as_raw_handle();
            inherited.extend([input_read, output_write, write]);
            stderr = Some(read);
        }
    }
    let handles: Vec<_> = inherited.iter().map(AsRawHandle::as_raw_handle).collect();
    let mut attrs = Attributes::new(2)?;
    // Attribute values remain alive through CreateProcess and attribute-list destruction.
    unsafe {
        attrs.set(
            PROC_THREAD_ATTRIBUTE_JOB_LIST,
            (&job_handle as *const HANDLE).cast(),
            size_of::<HANDLE>(),
        )?;
    }
    if let Some(console) = &terminal {
        unsafe {
            attrs.set(
                PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE,
                console.handle as *const c_void,
                size_of::<HPCON>(),
            )?;
        }
    } else {
        for &handle in &handles {
            check(unsafe {
                SetHandleInformation(handle, HANDLE_FLAG_INHERIT, HANDLE_FLAG_INHERIT)
            })?;
        }
        unsafe {
            attrs.set(
                PROC_THREAD_ATTRIBUTE_HANDLE_LIST,
                handles.as_ptr().cast(),
                size_of_val(handles.as_slice()),
            )?;
        }
    }
    startup.lpAttributeList = attrs.as_ptr();

    let path = launch
        .env
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case("PATH"))
        .map(|(_, value)| value);
    let executable =
        which::which_in(&launch.executable, path, &launch.cwd).map_err(io::Error::other)?;
    if executable
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("cmd") || ext.eq_ignore_ascii_case("bat"))
    {
        return Err(Error::invalid(
            "batch files require Command::Shell with cmd.exe",
        ));
    }
    let executable_wide = wide(executable.as_os_str())?;
    let cwd = wide(launch.cwd.as_os_str())?;
    let mut command_line = quote(executable.as_os_str())?;
    let is_cmd = executable
        .file_stem()
        .is_some_and(|stem| stem.eq_ignore_ascii_case("cmd"));
    for (index, arg) in launch.args.iter().enumerate() {
        command_line.push(b' ' as u16);
        // /s /c expects the command text inside one surrounding pair of quotes.
        if is_cmd
            && index == launch.args.len() - 1
            && launch.args.iter().any(|a| a.eq_ignore_ascii_case("/c"))
        {
            command_line.push(b'"' as u16);
            command_line.extend(wide(arg)?.into_iter().take_while(|c| *c != 0));
            command_line.push(b'"' as u16);
        } else {
            command_line.extend(quote(arg)?);
        }
    }
    command_line.push(0);
    let mut environment = Vec::new();
    for (key, value) in &launch.env {
        environment.extend(format!("{key}={value}").encode_utf16());
        environment.push(0);
    }
    environment.push(0);
    if environment.len() == 1 {
        environment.push(0);
    }
    let mut process: PROCESS_INFORMATION = unsafe { zeroed() };
    check(unsafe {
        CreateProcessW(
            executable_wide.as_ptr(),
            command_line.as_mut_ptr(),
            ptr::null(),
            ptr::null(),
            i32::from(terminal.is_none()),
            EXTENDED_STARTUPINFO_PRESENT
                | CREATE_UNICODE_ENVIRONMENT
                | if terminal.is_none() {
                    CREATE_NO_WINDOW
                } else {
                    0
                },
            environment.as_ptr().cast(),
            cwd.as_ptr(),
            &startup.StartupInfo,
            &mut process,
        )
    })?;
    let child = unsafe { OwnedHandle::from_raw_handle(process.hProcess) };
    let thread = unsafe { OwnedHandle::from_raw_handle(process.hThread) };
    drop(thread);
    drop(attrs);
    drop(inherited);
    let stream = if terminal.is_some() {
        OutputStream::Terminal
    } else {
        OutputStream::Stdout
    };
    let mut readers = vec![(stream, output_read)];
    if let Some(stderr) = stderr {
        readers.push((OutputStream::Stderr, stderr));
    }
    let writer = if matches!(launch.io, IoMode::Pipes { stdin: false }) {
        None
    } else {
        Some(input_write)
    };
    let control = Arc::new(WindowsControl { job, terminal });
    Ok(NativeProcess {
        control,
        child,
        readers,
        writer,
    })
}

fn create_job() -> Result<OwnedHandle> {
    let raw = unsafe { CreateJobObjectW(ptr::null(), ptr::null()) };
    if raw.is_null() {
        return Err(io::Error::last_os_error().into());
    }
    let job = unsafe { OwnedHandle::from_raw_handle(raw) };
    let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { zeroed() };
    limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
    check(unsafe {
        SetInformationJobObject(
            raw,
            JobObjectExtendedLimitInformation,
            (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
            size_of_val(&limits) as u32,
        )
    })?;
    Ok(job)
}

fn pipe() -> Result<(File, File)> {
    let mut read = ptr::null_mut();
    let mut write = ptr::null_mut();
    let security = SECURITY_ATTRIBUTES {
        nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: ptr::null_mut(),
        bInheritHandle: 0,
    };
    check(unsafe { CreatePipe(&mut read, &mut write, &security, 0) })?;
    Ok(unsafe { (File::from_raw_handle(read), File::from_raw_handle(write)) })
}

fn check(success: i32) -> Result<()> {
    if success == 0 {
        Err(io::Error::last_os_error().into())
    } else {
        Ok(())
    }
}

fn hresult(code: i32) -> Result<()> {
    if code < 0 {
        Err(Error::new(
            ErrorCode::Io,
            format!("Windows console error: 0x{:08x}", code as u32),
        ))
    } else {
        Ok(())
    }
}

struct Attributes {
    buffer: Vec<usize>,
}
impl Attributes {
    fn new(count: u32) -> Result<Self> {
        let mut bytes = 0;
        unsafe {
            InitializeProcThreadAttributeList(ptr::null_mut(), count, 0, &mut bytes);
        }
        let mut buffer = vec![0usize; bytes.div_ceil(size_of::<usize>())];
        check(unsafe {
            InitializeProcThreadAttributeList(buffer.as_mut_ptr().cast(), count, 0, &mut bytes)
        })?;
        Ok(Self { buffer })
    }
    fn as_ptr(&mut self) -> LPPROC_THREAD_ATTRIBUTE_LIST {
        self.buffer.as_mut_ptr().cast()
    }
    unsafe fn set(&mut self, key: u32, value: *const c_void, size: usize) -> Result<()> {
        check(unsafe {
            UpdateProcThreadAttribute(
                self.as_ptr(),
                0,
                key as usize,
                value,
                size,
                ptr::null_mut(),
                ptr::null(),
            )
        })
    }
}
impl Drop for Attributes {
    fn drop(&mut self) {
        unsafe {
            DeleteProcThreadAttributeList(self.as_ptr());
        }
    }
}

fn wide(value: &OsStr) -> Result<Vec<u16>> {
    let mut value: Vec<_> = value.encode_wide().collect();
    if value.contains(&0) {
        return Err(Error::invalid("process arguments contain a NUL character"));
    }
    value.push(0);
    Ok(value)
}

/// Windows C-runtime argument quoting: backslashes before quotes and the closing
/// quote are doubled. Shell scripts are interpreted only by the requested shell.
fn quote(value: &OsStr) -> Result<Vec<u16>> {
    let mut value = wide(value)?;
    value.pop();
    if !value.is_empty()
        && !value
            .iter()
            .any(|ch| matches!(*ch, 9 | 10 | 11 | 13 | 32 | 34))
    {
        return Ok(value);
    }
    let mut result = vec![b'"' as u16];
    let mut slashes = 0;
    for ch in value {
        if ch == b'\\' as u16 {
            slashes += 1;
            continue;
        }
        let count = if ch == b'"' as u16 {
            slashes * 2 + 1
        } else {
            slashes
        };
        result.extend(std::iter::repeat_n(b'\\' as u16, count));
        result.push(ch);
        slashes = 0;
    }
    result.extend(std::iter::repeat_n(b'\\' as u16, slashes * 2));
    result.push(b'"' as u16);
    Ok(result)
}
