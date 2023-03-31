use anyhow::{bail, Result};
use bytes::Bytes;

use super::{FileType, Id, ReadBackend, WriteBackend};
use super::{LocalBackend, RcloneBackend, RestBackend, S3Backend};

#[derive(Clone)]
pub enum ChooseBackend {
    Local(LocalBackend),
    Rest(RestBackend),
    Rclone(RcloneBackend),
    S3(S3Backend),
}

use ChooseBackend::{Local, Rclone, Rest, S3};

impl ChooseBackend {
    pub fn from_url(url: &str) -> Result<Self> {
        Ok(match url.split_once(':') {
            #[cfg(windows)]
            Some((drive, _)) if drive.len() == 1 => Local(LocalBackend::new(url)?),
            Some(("rclone", path)) => Rclone(RcloneBackend::new(path)?),
            Some(("rest", path)) => Rest(RestBackend::new(path)?),
            Some(("local", path)) => Local(LocalBackend::new(path)?),
            Some(("s3", path)) => S3(S3Backend::new(path)?),
            Some((backend, _)) => bail!("backend {backend} is not supported!"),
            None => Local(LocalBackend::new(url)?),
        })
    }
}

impl ReadBackend for ChooseBackend {
    fn location(&self) -> String {
        match self {
            Local(local) => local.location(),
            Rest(rest) => rest.location(),
            Rclone(rclone) => rclone.location(),
            S3(s3) => s3.location(),
        }
    }

    fn set_option(&mut self, option: &str, value: &str) -> Result<()> {
        match self {
            Local(local) => local.set_option(option, value),
            Rest(rest) => rest.set_option(option, value),
            Rclone(rclone) => rclone.set_option(option, value),
            S3(s3) => s3.set_option(option, value),
        }
    }

    fn list_with_size(&self, tpe: FileType) -> Result<Vec<(Id, u32)>> {
        match self {
            Local(local) => local.list_with_size(tpe),
            Rest(rest) => rest.list_with_size(tpe),
            Rclone(rclone) => rclone.list_with_size(tpe),
            S3(s3) => s3.list_with_size(tpe),
        }
    }

    fn read_full(&self, tpe: FileType, id: &Id) -> Result<Bytes> {
        match self {
            Local(local) => local.read_full(tpe, id),
            Rest(rest) => rest.read_full(tpe, id),
            Rclone(rclone) => rclone.read_full(tpe, id),
            S3(s3) => s3.read_full(tpe, id),
        }
    }

    fn read_partial(
        &self,
        tpe: FileType,
        id: &Id,
        cacheable: bool,
        offset: u32,
        length: u32,
    ) -> Result<Bytes> {
        match self {
            Local(local) => local.read_partial(tpe, id, cacheable, offset, length),
            Rest(rest) => rest.read_partial(tpe, id, cacheable, offset, length),
            Rclone(rclone) => rclone.read_partial(tpe, id, cacheable, offset, length),
            S3(s3) => s3.read_partial(tpe, id, cacheable, offset, length),
        }
    }
}

impl WriteBackend for ChooseBackend {
    fn create(&self) -> Result<()> {
        match self {
            Local(local) => local.create(),
            Rest(rest) => rest.create(),
            Rclone(rclone) => rclone.create(),
            S3(s3) => s3.create(),
        }
    }

    fn write_bytes(&self, tpe: FileType, id: &Id, cacheable: bool, buf: Bytes) -> Result<()> {
        match self {
            Local(local) => local.write_bytes(tpe, id, cacheable, buf),
            Rest(rest) => rest.write_bytes(tpe, id, cacheable, buf),
            Rclone(rclone) => rclone.write_bytes(tpe, id, cacheable, buf),
            S3(s3) => s3.write_bytes(tpe, id, cacheable, buf),
        }
    }

    fn remove(&self, tpe: FileType, id: &Id, cacheable: bool) -> Result<()> {
        match self {
            Local(local) => local.remove(tpe, id, cacheable),
            Rest(rest) => rest.remove(tpe, id, cacheable),
            Rclone(rclone) => rclone.remove(tpe, id, cacheable),
            S3(s3) => s3.remove(tpe, id, cacheable),
        }
    }
}
