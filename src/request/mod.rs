extern crate anyhow;
use anyhow::Result;
use tracing::info;

use broker_proto::Protocol;
use bollard::Docker;

use futures_util::stream::StreamExt;
use futures_util::stream::TryStreamExt;

#[allow(unused_imports)]
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use rmp_serde::{Deserializer, Serializer};

extern crate serde;
extern crate rmp_serde as rmps;

/*
#[derive(Debug, Error)]
enum ProtocolError {
    NameError,
    //OtherError
}
*/

pub async fn handle_request(buf: Vec<u8>) -> Result<Vec<u8>> {

    let docker =  if let Ok(d) = Docker::connect_with_local_defaults() {
        d
    } else {
        let resp = Protocol::error_none("Docker not running.");

        let mut output = Vec::new();
        resp.serialize(&mut Serializer::new(&mut output))?;

        return Ok(output);
    };

    use std::convert::TryFrom;
    let request: Protocol = if let Ok(req) = broker_proto::Protocol::try_from(&buf[..]) {
        req
    } else {
        let resp = Protocol::error_none("Invalid request format.");

        let mut output = Vec::new();
        resp.serialize(&mut Serializer::new(&mut output))?;

        return Ok(output);
    };

    let resp = match request.packet_type {
        broker_proto::Type::Transfer => Protocol::error_none("Not implemented."),
        broker_proto::Type::Response => {
            Protocol::error_none("Server cannot receive Response.")
        },
        broker_proto::Type::Command(cmd) => {
            match cmd.cmd_type {
                broker_proto::CommandType::List => {

                    match list_containers(&docker).await {
                        Ok(res) => res,
                        Err(e) => Protocol::error_none(&e.to_string())
                    }
                    
                },
                broker_proto::CommandType::Change => {
                    if let None = cmd.name {
                        Protocol::error_none("No name parameter given")
                    } else {
                        match get_container_changes(&docker, &cmd.name.unwrap()).await {
                            Ok(res) => res,
                            Err(e) => Protocol::error_none(&e.to_string())
                        }
                    }
                    
                },
                broker_proto::CommandType::Container => {
                    if let None = cmd.name {
                        Protocol::error_none("No name parameter given")
                    } else {
                        match inspect_container(&docker, &cmd.name.unwrap()).await {
                            Ok(res) => res,
                            Err(e) => Protocol::error_none(&e.to_string())
                        }
                    }
                },
                broker_proto::CommandType::Stats => {
                    if let None = cmd.name {
                        Protocol::error_none("No name parameter given")
                    } else {
                        match get_stats(&docker, &cmd.name.unwrap()).await {
                            Ok(res) => res,
                            Err(e) => Protocol::error_none(&e.to_string())
                        }
                    }
                },
                broker_proto::CommandType::Top => {
                    if let None = cmd.name {
                        Protocol::error_none("No name parameter given")
                    } else {
                        match container_top(&docker, &cmd.name.unwrap()).await {
                            Ok(res) => res,
                            Err(e) => Protocol::error_none(&e.to_string())
                        }
                    }
                },
                broker_proto::CommandType::Log => {
                    if let None = cmd.name {
                        Protocol::error_none("No name parameter given")
                    } else {
                        match get_logs(&docker, &cmd.name.unwrap()).await {
                            Ok(res) => res,
                            Err(e) => Protocol::error_none(&e.to_string())
                        }
                    }
                },
                broker_proto::CommandType::Stop => {
                    if let None = cmd.name {
                        Protocol::error_none("No name parameter given")
                    } else {
                        match stop_container(&docker, &cmd.name.unwrap()).await {
                            Ok(res) => res,
                            Err(e) => Protocol::error_none(&e.to_string())
                        }
                    }
                },
                broker_proto::CommandType::Start => {
                    if let None = cmd.name {
                        Protocol::error_none("No name parameter given")
                    } else {
                        match start_container(&docker, &cmd.name.unwrap()).await {
                            Ok(res) => res,
                            Err(e) => Protocol::error_none(&e.to_string())
                        }
                    }
                },
                broker_proto::CommandType::Kill => {
                    if let None = cmd.name {
                        Protocol::error_none("No name parameter given")
                    } else {
                        match kill_container(&docker, &cmd.name.unwrap()).await {
                            Ok(res) => res,
                            Err(e) => Protocol::error_none(&e.to_string())
                        }
                    }
                },
                broker_proto::CommandType::Restart => {
                    if let None = cmd.name {
                        Protocol::error_none("No name parameter given")
                    } else {
                        match restart_container(&docker, &cmd.name.unwrap()).await {
                            Ok(res) => res,
                            Err(e) => Protocol::error_none(&e.to_string())
                        }
                    }
                },
                broker_proto::CommandType::Prune => {

                    match prune_container(&docker).await {
                        Ok(res) => res,
                        Err(e) => Protocol::error_none(&e.to_string())
                    }
                    
                },
                broker_proto::CommandType::Remove => {
                    if let None = cmd.name {
                        Protocol::error_none("No name parameter given")
                    } else {
                        match remove_container(&docker, &cmd.name.unwrap()).await {
                            Ok(res) => res,
                            Err(e) => Protocol::error_none(&e.to_string())
                        }
                    }
                },
                broker_proto::CommandType::Update => {
                    if let None = cmd.name {
                        Protocol::error_none("No name parameter given")
                    } else {
                        match update_container(&docker, &cmd.name.unwrap()).await {
                            Ok(res) => res,
                            Err(e) => Protocol::error_none(&e.to_string())
                        }
                    }
                },
                broker_proto::CommandType::Create => {

                    match create_container(&docker).await {
                        Ok(res) => res,
                        Err(e) => Protocol::error_none(&e.to_string())
                    }
                },
                _ => Protocol::error_none("Not implemented"),
            }
        }
        broker_proto::Type::Other => Protocol::error_none("Not implemented.")

    };

    info!(content = %format!("{:#?}", &resp));

    let mut output = Vec::new();

    resp.serialize(&mut Serializer::new(&mut output))?;

    Ok(output)
}

async fn list_containers(docker: &Docker) -> Result<Protocol> {
    let containers: &Vec<bollard::container::APIContainers> = &docker.list_containers(
        Some(bollard::container::ListContainersOptions::<String>{
            all: true,
            ..Default::default()
    })).await?;

    let containers = containers
        .iter()
        .map(|el| broker_proto::ContainerList{0: el.clone()})
        .collect::<Vec<_>>();

    let mut proto = Protocol::response(&docker).await?;
    proto.body = broker_proto::Body::ContainerList(containers);

    Ok(proto)
}

async fn get_container_changes(docker: &Docker, name: &str) -> Result<Protocol> {
    let containers = &docker.container_changes(name).await?;

    let output = if let Some(changes) = containers {
        Some(changes
            .iter()
            .map(|el| broker_proto::Change{0: el.clone()})
            .collect::<Vec<_>>())
    } else {
        None
    };

    let mut proto = Protocol::response(&docker).await?;
    proto.body = broker_proto::Body::Change(output);

    Ok(proto)
}

async fn inspect_container(docker: &Docker, name: &str) -> Result<Protocol> {
    let containers = docker
        .inspect_container(name, None::<bollard::container::InspectContainerOptions>)
        .await?;

    let mut proto = Protocol::response(&docker).await?;
    proto.body = broker_proto::Body::Container(broker_proto::Container{0: containers});

    Ok(proto)
}

async fn get_stats(docker: &Docker, name: &str) -> Result<Protocol> {
    let containers = docker
        .stats(name, Some(bollard::container::StatsOptions {
            stream: false
        }))
        .take(1)
        .try_collect::<Vec<_>>()
        .await?;

    let containers = containers
        .iter()
        .map(|el| broker_proto::Stats{0: el.clone()})
        .collect::<Vec<_>>();

    let mut proto = Protocol::response(&docker).await?;
    proto.body = broker_proto::Body::Stats(containers);

    Ok(proto)
}

async fn container_top(docker: &Docker, name: &str) -> Result<Protocol> {
    let containers = docker
        .top_processes(name, Some(bollard::container::TopOptions{
            ps_args: "aux"
        }))
        .await?;

    let mut proto = Protocol::response(&docker).await?;
    proto.body = broker_proto::Body::TopResult(broker_proto::TopResult{0: containers});

    Ok(proto)
}

async fn get_logs(docker: &Docker, name: &str) -> Result<Protocol> {
    let containers = docker
        .logs(name, Some(bollard::container::LogsOptions {
            follow: false,
            stdout: true,
            stderr: true,
            tail: String::from("all"),
            ..Default::default()
        }))
        //.take(1)
        .try_collect::<Vec<_>>()
        .await?;

    let containers = containers
        .iter()
        .map(|el| {
            match el {
                bollard::container::LogOutput::Console{message:msg} => broker_proto::LogOutputWrapper::Console {message: msg.clone()},
                bollard::container::LogOutput::StdIn{message:msg} => broker_proto::LogOutputWrapper::StdIn {message: msg.clone()},
                bollard::container::LogOutput::StdErr{message:msg} => broker_proto::LogOutputWrapper::StdErr {message: msg.clone()},
                bollard::container::LogOutput::StdOut{message:msg} => broker_proto::LogOutputWrapper::StdOut {message: msg.clone()}
            }
        })
        .collect::<Vec<_>>();

    let mut proto = Protocol::response(&docker).await?;
    proto.body = broker_proto::Body::LogOutput(containers);

    Ok(proto)
}

async fn stop_container(docker: &Docker, name: &str) -> Result<Protocol> {
    docker
        .stop_container(name, Some(bollard::container::StopContainerOptions {
            t: 32
    })).await?;

    Ok(Protocol::response(&docker).await?)
}

async fn start_container(docker: &Docker, name: &str) -> Result<Protocol> {
    docker
        .start_container(name, Some(bollard::container::StartContainerOptions {
            detach_keys: "ctrl-^"
        })).await?;

    Ok(Protocol::response(&docker).await?)
}

async fn kill_container(docker: &Docker, name: &str) -> Result<Protocol> {
    docker
        .kill_container(name, Some(bollard::container::KillContainerOptions {
            signal: "SIGINT",
        })).await?;

    Ok(Protocol::response(&docker).await?)
}

async fn restart_container(docker: &Docker, name: &str) -> Result<Protocol> {
    docker
        .restart_container(name, Some(bollard::container::RestartContainerOptions {
            t: 30,
        })).await?;

    Ok(Protocol::response(&docker).await?)
}

async fn prune_container(docker: &Docker) -> Result<Protocol> {
    use std::collections::HashMap;

    let mut filters = HashMap::new();
    filters.insert("until", vec!("10m"));

    let res = docker.prune_containers(Some(bollard::container::PruneContainersOptions {
        filters: filters
    }))
    .await?;

    let mut proto = Protocol::response(&docker).await?;
    proto.body = broker_proto::Body::PrunedContainers(broker_proto::PruneContainerResults {0: res});

    Ok(proto)
}

async fn remove_container(docker: &Docker, name: &str) -> Result<Protocol> {
    docker
        .remove_container(name, Some(bollard::container::RemoveContainerOptions {
            force: true,
            ..Default::default()
    })).await?;

    Ok(Protocol::response(&docker).await?)
}

async fn update_container(docker: &Docker, name: &str) -> Result<Protocol> {
    docker
        .update_container(name, bollard::container::UpdateContainerOptions {
            memory: Some(314572800),
            memory_swap: Some(314572800),
            ..Default::default()
    }).await?;

    Ok(Protocol::response(&docker).await?)
}

async fn create_container(docker: &Docker) -> Result<Protocol> {
    let options = Some(bollard::container::CreateContainerOptions{
        name: "my-new-container",
    });

    let config = bollard::container::Config {
        image: Some("hello-world"),
        cmd: Some(vec!["/hello"]),
        ..Default::default()
    };

    let res = docker.create_container(options, config).await?;
    let mut proto = Protocol::response(&docker).await?;
    proto.body = broker_proto::Body::CreateContainerResults(broker_proto::CreateContainerResults{0: res});

    Ok(proto)
}