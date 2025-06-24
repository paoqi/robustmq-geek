
use super::path_list;
use super::{index::index, v1_path};
use axum::routing::{get, post};
use axum::Router;
use common_base::config::placement_center::placement_center_conf;
use log::info;
use std::net::SocketAddr;
use tokio::{select, sync::broadcast};
use common_base::http_response::success_response;

pub const ROUTE_ROOT: &str = "/index";

#[derive(Clone)]
pub struct HttpServerStateTest {
    pub name:String,
}


pub async fn start_http_server_test(stop_sx: broadcast::Sender<bool>) {
    

    let config = placement_center_conf();
    let ip: SocketAddr = match format!("0.0.0.0:{}", config.http_port).parse() {
        Ok(data) => data,
        Err(e) => {
            panic!("{}", e);
        }
    };

    info!("Broker HTTP Server start. port:{}", config.http_port);
    let state = HttpServerStateTest {name:"yangqi".to_string()  };
    let app = routes_test(state);

    let mut stop_rx = stop_sx.subscribe();

    let listener = match tokio::net::TcpListener::bind(ip).await {
        Ok(data) => data,
        Err(e) => {
            panic!("{}", e);
        }
    };

    select! {
        val = stop_rx.recv() =>{
            match val{
                Ok(flag) => {
                    if flag {
                        info!("HTTP Server stopped successfully");

                    }
                }
                Err(_) => {}
            }
        },
        val = axum::serve(listener, app.clone())=>{
            match val{
                Ok(()) => {
                },
                Err(e) => {
                    panic!("{}",e);
                }
            }
        }
    }
}

fn routes_test(state: HttpServerStateTest) -> Router {
    let common = Router::new()
        .route(&v1_path(&path_list(ROUTE_ROOT)), get(index_test));

    let app = Router::new().merge(common);
    return app.with_state(state);
}

pub async fn index_test() -> String {
    // println!("name:{}",state.name);
    return success_response("hello,world");
}