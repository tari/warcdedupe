
/// A file that is opened on-demand when reading or writing.
///
/// Can also be explicitly opened and closed, if the application wishes to
/// keep a bounded number of files open at the same time.
pub struct JitFile<P> {
    path: P,
    options: OpenOptions,
    file: Option<File>,
    offset: usize,
}

impl<P: AsRef<Path>> JitFile<P> {
    pub fn new(path: P, options: OpenOptions) -> Self {
        JitFile {
            path: path,
            options: options,
            file: None,
            offset: 0,
        }
    }

    pub fn open(&mut self) -> io::Result<&mut File> {
        self.file = Some(self.options.open(&self.path)?);
        Ok(self.file.as_mut().unwrap())
    }

    pub fn close(&mut self) {
        self.file = None;
    }
}

