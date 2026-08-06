use anyhow::Result;
use bollard::{
    Docker,
    errors::Error,
    query_parameters::CreateImageOptionsBuilder,
};

use futures_util::TryStreamExt;

pub async fn pull_if_needed(
    docker: &Docker,
    image: &str,
) -> Result<()> {
    match docker.inspect_image(image).await {
        Ok(_) => return Ok(()),

        Err(Error::DockerResponseServerError { status_code: 404, .. }) => {}

        Err(e) => return Err(e.into()),
    }

    println!("Pulling {image}");

    let options = CreateImageOptionsBuilder::default()
        .from_image(image)
        .build();

    let mut stream = docker.create_image(
        Some(options),
        None,
        None,
    );

    while let Some(progress) = stream.try_next().await? {
        if let Some(status) = progress.status {
            println!("{status}");
        }
    }

    Ok(())
}