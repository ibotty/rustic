use std::str::FromStr;
use std::time::Duration;

use anyhow::{bail, Result};
use backoff::{backoff::Backoff, Error, ExponentialBackoff, ExponentialBackoffBuilder};
use s3::creds::Credentials;
use s3::Bucket;
use s3::request_trait::ResponseData;
use s3::error::S3Error;

use bytes::Bytes;
use log::*;
use serde::Deserialize;

use super::{FileType, Id, ReadBackend, WriteBackend};

// trait CheckError to add user-defined methoed check_error on Response
trait CheckError {
    fn check_error(self) -> std::result::Result<ResponseData, Error<S3Error>>;
}

impl CheckError for std::result::Result<ResponseData, S3Error> {
    // Check s3 Response for error and treat errors as permanent or transient
    fn check_error(self) -> std::result::Result<ResponseData, Error<S3Error>> {
        match self.status_code() {
            200 => Ok(self),
            // Note: status() always give Some(_) as it is called from a Response
            // Err(err) if err.status().unwrap().is_client_error() => Err(Error::Permanent(err)),
            // Err(err) => Err(Error::Transient {
            //     err,
            //     retry_after: None,
            // }),
        }
    }
}

#[derive(Clone)]
struct MaybeBackoff(Option<ExponentialBackoff>);

impl Backoff for MaybeBackoff {
    fn next_backoff(&mut self) -> Option<Duration> {
        self.0.as_mut().and_then(|back| back.next_backoff())
    }

    fn reset(&mut self) {
        if let Some(b) = self.0.as_mut() {
            b.reset();
        }
    }
}

#[derive(Clone)]
pub struct S3Backend {
    bucket: Bucket,
    prefix: String,
    backoff: MaybeBackoff,
}

fn notify(err: S3Error, duration: Duration) {
    warn!("Error {err} at {duration:?}, retrying");
}

impl S3Backend {
    pub fn new(url: &str) -> Result<Self> {
        let aws_creds = Credentials::default()?;
        let region = "eu-central-1".parse()?;
        let bucket_name = url;
        let prefix = "".to_string();
        let bucket = Bucket::new(bucket_name, region, aws_creds)?;

        Ok(Self {
            bucket,
            prefix,
            backoff: MaybeBackoff(Some(
                ExponentialBackoffBuilder::new()
                    .with_max_elapsed_time(Some(Duration::from_secs(600)))
                    .build(),
            )),
        })
    }

    fn url(&self, tpe: FileType, id: &Id) -> Result<String> {
        let id_path = match tpe {
            FileType::Config => "config".to_string(),
            _ => {
                let hex_id = id.to_hex();
                let mut path = tpe.name().to_string();
                path.push('/');
                path.push_str(&hex_id);
                path
            }
        };
        Ok(format!("{}{}", self.prefix, &id_path))
    }
}

impl ReadBackend for S3Backend {
    fn location(&self) -> String {
        format!("s3:{}/{}/{}", self.bucket.endpoint_url, self.bucket.bucket_name, self.prefix)
    }

    fn set_option(&mut self, option: &str, value: &str) -> Result<()> {
        if option == "retry" {
            match value {
                "true" => {
                    self.backoff = MaybeBackoff(Some(
                        ExponentialBackoffBuilder::new()
                            .with_max_elapsed_time(Some(Duration::from_secs(120)))
                            .build(),
                    ));
                }
                "false" => {
                    self.backoff = MaybeBackoff(None);
                }
                val => bail!("value {val} not supported for option retry!"),
            }
        } else if option == "timeout" {
            let timeout = humantime::Duration::from_str(value)?;
            self.client = ClientBuilder::new().timeout(*timeout).build()?;
        }
        Ok(())
    }

    fn list_with_size(&self, tpe: FileType) -> Result<Vec<(Id, u32)>> {
        trace!("listing tpe: {tpe:?}");
        let url = if tpe == FileType::Config {
            self.url.join("config")?
        } else {
            let mut path = tpe.name().to_string();
            path.push('/');
            self.url.join(&path)?
        };

        Ok(backoff::retry_notify(
            self.backoff.clone(),
            || {
                if tpe == FileType::Config {
                    return Ok(
                        match self.client.head(url.clone()).send()?.status().is_success() {
                            true => vec![(Id::default(), 0)],
                            false => Vec::new(),
                        },
                    );
                }

                // format which is delivered by the REST-service
                #[derive(Deserialize)]
                struct ListEntry {
                    name: String,
                    size: u32,
                }

                let list = self
                    .client
                    .get(url.clone())
                    .header("Accept", "application/vnd.x.restic.rest.v2")
                    .send()?
                    .check_error()?
                    .json::<Vec<ListEntry>>()?;
                Ok(list
                    .into_iter()
                    .filter_map(|i| match Id::from_hex(&i.name) {
                        Ok(id) => Some((id, i.size)),
                        Err(_) => None,
                    })
                    .collect())
            },
            notify,
        )?)
    }

    fn read_full(&self, tpe: FileType, id: &Id) -> Result<Bytes> {
        trace!("reading tpe: {tpe:?}, id: {id}");
        let url = self.url(tpe, id)?;
        Ok(backoff::retry_notify(
            self.backoff.clone(),
            || {
                Ok(Bytes::from(self
                    .bucket
                    .get_object(url)
                    .check_error()?
                    .bytes()))
            },
            notify,
        )?)
    }

    fn read_partial(
        &self,
        tpe: FileType,
        id: &Id,
        _cacheable: bool,
        offset: u32,
        length: u32,
    ) -> Result<Bytes> {
        trace!("reading tpe: {tpe:?}, id: {id}, offset: {offset}, length: {length}");
        let offset2 = offset + length - 1;
        let header_value = format!("bytes={offset}-{offset2}");
        let url = self.url(tpe, id)?;
        Ok(backoff::retry_notify(
            self.backoff.clone(),
            || {
                Ok(Bytes::from(self
                    .bucket
                    .get_object_range(url, offset.into(), Some(length.into()))
                    .check_error()?
                    .bytes()))
            },
            notify,
        )?)
    }
}

impl WriteBackend for S3Backend {
    fn create(&self) -> Result<()> {
        let url = self.url.join("?create=true")?;
        Ok(backoff::retry_notify(
            self.backoff.clone(),
            || {
                self.client.post(url.clone()).send()?.check_error()?;
                Ok(())
            },
            notify,
        )?)
    }

    fn write_bytes(&self, tpe: FileType, id: &Id, _cacheable: bool, buf: Bytes) -> Result<()> {
        trace!("writing tpe: {:?}, id: {}", &tpe, &id);
        let url = self.url(tpe, id)?;
        Ok(backoff::retry_notify(
            self.backoff.clone(),
            || {
                self.bucket.put_object(url, &buf).check_error()?;
                Ok(())
            },
            notify,
        )?)
    }

    fn remove(&self, tpe: FileType, id: &Id, _cacheable: bool) -> Result<()> {
        trace!("removing tpe: {:?}, id: {}", &tpe, &id);
        let url = self.url(tpe, id)?;
        Ok(backoff::retry_notify(
            self.backoff.clone(),
            || {
                self.bucket.delete_object(url).check_error()?;
                Ok(())
            },
            notify,
        )?)
    }
}
