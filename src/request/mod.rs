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

#[allow(unused_imports)]
use bollard::container::*;

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

                    if let Some(arg) = cmd.argument {
                        if let broker_proto::Arguments::ContainerChanges{name} = arg {
                            match get_container_changes(&docker, &name).await {
                                Ok(res) => res,
                                Err(e) => Protocol::error_none(&e.to_string())
                            }
                        } else {
                            Protocol::error_none("Invalid argument.")
                        }
                    } else {
                        Protocol::error_none("No parameter received.")
                    }
                },
                broker_proto::CommandType::Container => {
                    if let Some(arg) = cmd.argument {
                        if let broker_proto::Arguments::InspectContainer{name, options} = arg {
                            match inspect_container(&docker, &name, options).await {
                                Ok(res) => res,
                                Err(e) => Protocol::error_none(&e.to_string())
                            }
                        } else {
                            Protocol::error_none("Invalid argument.")
                        }
                    } else {
                        Protocol::error_none("No parameter received.")
                    }
                },
                broker_proto::CommandType::Stats => {
                    if let Some(arg) = cmd.argument {
                        if let broker_proto::Arguments::Stats{name, options} = arg {
                            match get_stats(&docker, &name, options).await {
                                Ok(res) => res,
                                Err(e) => Protocol::error_none(&e.to_string())
                            }
                        } else {
                            Protocol::error_none("Invalid argument.")
                        }
                    } else {
                        Protocol::error_none("No parameter received.")
                    }
                },
                broker_proto::CommandType::Top => {
                    if let Some(arg) = cmd.argument {
                        if let broker_proto::Arguments::Top{name, options} = arg {
                            match container_top(&docker, &name, options).await {
                                Ok(res) => res,
                                Err(e) => Protocol::error_none(&e.to_string())
                            }
                        } else {
                            Protocol::error_none("Invalid argument.")
                        }
                    } else {
                        Protocol::error_none("No parameter received.")
                    }
                },
                broker_proto::CommandType::Log => {
                    if let Some(arg) = cmd.argument {
                        if let broker_proto::Arguments::Logs{name, options} = arg {
                            match get_logs(&docker, &name, options).await {
                                Ok(res) => res,
                                Err(e) => Protocol::error_none(&e.to_string())
                            }
                        } else {
                            Protocol::error_none("Invalid argument.")
                        }
                    } else {
                        Protocol::error_none("No parameter received.")
                    }
                },
                broker_proto::CommandType::Stop => {
                    if let Some(arg) = cmd.argument {
                        if let broker_proto::Arguments::Stop{name, options} = arg {
                            match stop_container(&docker, &name, options).await {
                                Ok(res) => res,
                                Err(e) => Protocol::error_none(&e.to_string())
                            }
                        } else {
                            Protocol::error_none("Invalid argument.")
                        }
                    } else {
                        Protocol::error_none("No parameter received.")
                    }
                },
                broker_proto::CommandType::Start => {
                    if let Some(arg) = cmd.argument {
                        if let broker_proto::Arguments::Start{name, options} = arg {
                            match start_container(&docker, &name, options).await {
                                Ok(res) => res,
                                Err(e) => Protocol::error_none(&e.to_string())
                            }
                        } else {
                            Protocol::error_none("Invalid argument.")
                        }
                    } else {
                        Protocol::error_none("No parameter received.")
                    }
                },
                broker_proto::CommandType::Kill => {
                    if let Some(arg) = cmd.argument {
                        if let broker_proto::Arguments::Kill{name, options} = arg {
                            match kill_container(&docker, &name, options).await {
                                Ok(res) => res,
                                Err(e) => Protocol::error_none(&e.to_string())
                            }
                        } else {
                            Protocol::error_none("Invalid argument.")
                        }
                    } else {
                        Protocol::error_none("No parameter received.")
                    }
                },
                broker_proto::CommandType::Restart => {
                    if let Some(arg) = cmd.argument {
                        if let broker_proto::Arguments::Restart{name, options} = arg {
                            match restart_container(&docker, &name, options).await {
                                Ok(res) => res,
                                Err(e) => Protocol::error_none(&e.to_string())
                            }
                        } else {
                            Protocol::error_none("Invalid argument.")
                        }
                    } else {
                        Protocol::error_none("No parameter received.")
                    }
                },
                broker_proto::CommandType::Prune => {
                    if let Some(arg) = cmd.argument {
                        if let broker_proto::Arguments::Prune{options} = arg {
                            match prune_container(&docker, options).await {
                                Ok(res) => res,
                                Err(e) => Protocol::error_none(&e.to_string())
                            }
                        } else {
                            Protocol::error_none("Invalid argument.")
                        }
                    } else {
                        Protocol::error_none("No parameter received.")
                    }
                },
                broker_proto::CommandType::Remove => {
                    if let Some(arg) = cmd.argument {
                        if let broker_proto::Arguments::Remove{name, options} = arg {
                            match remove_container(&docker, &name, options).await {
                                Ok(res) => res,
                                Err(e) => Protocol::error_none(&e.to_string())
                            }
                        } else {
                            Protocol::error_none("Invalid argument.")
                        }
                    } else {
                        Protocol::error_none("No parameter received.")
                    }
                },
                broker_proto::CommandType::Update => {
                    if let Some(arg) = cmd.argument {
                        if let broker_proto::Arguments::Update{name, options} = arg {
                            match update_container(&docker, &name, options).await {
                                Ok(res) => res,
                                Err(e) => Protocol::error_none(&e.to_string())
                            }
                        } else {
                            Protocol::error_none("Invalid argument.")
                        }
                    } else {
                        Protocol::error_none("No parameter received.")
                    }
                },
                broker_proto::CommandType::Create => {
                    if let Some(arg) = cmd.argument {
                        if let broker_proto::Arguments::Create{config, options} = arg {
                            match create_container(&docker, config, options).await {
                                Ok(res) => res,
                                Err(e) => Protocol::error_none(&e.to_string())
                            }
                        } else {
                            Protocol::error_none("Invalid argument.")
                        }
                    } else {
                        Protocol::error_none("No parameter received.")
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
    let containers = docker.list_containers(
        Some(bollard::container::ListContainersOptions::<String>{
            all: true,
            ..Default::default()
    })).await?;

  

    let mut proto = Protocol::response(&docker).await?;
    proto.body = broker_proto::Body::ContainerList(containers);

    Ok(proto)
}

async fn get_container_changes(docker: &Docker, name: &str) -> Result<Protocol> {
    let containers = docker.container_changes(name).await?;

    

    let mut proto = Protocol::response(&docker).await?;
    proto.body = broker_proto::Body::Change(containers);

    Ok(proto)
}

async fn inspect_container(docker: &Docker, name: &str, opt: Option<InspectContainerOptions>) -> Result<Protocol> {
    let containers = docker
        .inspect_container(name, opt)
        .await?;

    let mut proto = Protocol::response(&docker).await?;
    proto.body = broker_proto::Body::Container(containers);

    Ok(proto)
}

async fn get_stats(docker: &Docker, name: &str, opt: Option<StatsOptions>) -> Result<Protocol> {
    let containers = docker
        .stats(name, opt)
        .take(1)
        .try_collect::<Vec<_>>()
        .await?;

    let mut proto = Protocol::response(&docker).await?;
    proto.body = broker_proto::Body::Stats(containers);

    Ok(proto)
}

async fn container_top(docker: &Docker, name: &str, opt: Option<TopOptions<String>>) -> Result<Protocol> {
    let containers = docker
        .top_processes(name, opt)
        .await?;

    let mut proto = Protocol::response(&docker).await?;
    proto.body = broker_proto::Body::TopResult(containers);

    Ok(proto)
}

async fn get_logs(docker: &Docker, name: &str, opt: Option<LogsOptions>) -> Result<Protocol> {
    let containers = docker
        .logs(name, opt)
        //.take(1)
        .try_collect::<Vec<_>>()
        .await?;

    let mut proto = Protocol::response(&docker).await?;
    proto.body = broker_proto::Body::LogOutput(containers);

    Ok(proto)
}

async fn stop_container(docker: &Docker, name: &str, opt: Option<StopContainerOptions>) -> Result<Protocol> {
    docker
        .stop_container(name, opt).await?;

    Ok(Protocol::response(&docker).await?)
}

async fn start_container(docker: &Docker, name: &str, opt: Option<StartContainerOptions<String>>) -> Result<Protocol> {
    docker
        .start_container(name, opt).await?;

    Ok(Protocol::response(&docker).await?)
}

async fn kill_container(docker: &Docker, name: &str, opt: Option<KillContainerOptions<String>>) -> Result<Protocol> {
    docker
        .kill_container(name, opt).await?;

    Ok(Protocol::response(&docker).await?)
}

async fn restart_container(docker: &Docker, name: &str, opt: Option<RestartContainerOptions>) -> Result<Protocol> {
    docker
        .restart_container(name, opt).await?;

    Ok(Protocol::response(&docker).await?)
}

async fn prune_container(docker: &Docker, opt: Option<PruneContainersOptions<String>>) -> Result<Protocol> {
    let res = docker.prune_containers(opt)
    .await?;

    let mut proto = Protocol::response(&docker).await?;
    proto.body = broker_proto::Body::PrunedContainers(res);

    Ok(proto)
}

async fn remove_container(docker: &Docker, name: &str, opt: Option<RemoveContainerOptions>) -> Result<Protocol> {
    docker
        .remove_container(name, opt).await?;

    Ok(Protocol::response(&docker).await?)
}

async fn update_container(docker: &Docker, name: &str, opt: UpdateContainerOptions) -> Result<Protocol> {
    docker
        .update_container(name, opt).await?;

    Ok(Protocol::response(&docker).await?)
}

async fn create_container(docker: &Docker, config: Config<String>, opt: Option<CreateContainerOptions<String>>) -> Result<Protocol> {

    let res = docker.create_container(opt, config).await?;
    let mut proto = Protocol::response(&docker).await?;
    proto.body = broker_proto::Body::CreateContainerResults(res);

    Ok(proto)
}