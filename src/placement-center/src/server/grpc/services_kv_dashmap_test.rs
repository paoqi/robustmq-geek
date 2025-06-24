use dashmap::DashMap;
use protocol::kv::{
    CommonReply, DeleteRequest, ExistsReply, ExistsRequest, GetReply, GetRequest, SetRequest,
    kv_service_server::KvService,
};
use tonic::{Request, Response, Status};
use common_base::config::placement_center::placement_center_conf;
use tokio::{select, sync::broadcast};
use log::info;
use protocol::{
    kv::kv_service_server::KvServiceServer,
    openraft::open_raft_service_server::OpenRaftServiceServer,
    placement::placement_center_service_server::PlacementCenterServiceServer,
};
use tonic::transport::Server;



pub struct GrpcKvServicesTest {
    // 初始化⼀个基于 DashMap 库的 HashMap
    data: DashMap<String, String>,
}

impl GrpcKvServicesTest {
    pub fn new() -> Self {
        GrpcKvServicesTest {
            data: DashMap::with_capacity(5),
        }
    }
}

#[tonic::async_trait]
impl KvService for GrpcKvServicesTest {
    async fn set(&self, request: Request<SetRequest>) -> Result<Response<CommonReply>, Status> {
        let req = request.into_inner();
        self.data.insert(req.key, req.value);
        Ok(Response::new(CommonReply::default()))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetReply>, Status> {
        let req = request.into_inner();
        if let Some(v) = self.data.get(&req.key) {
            return Ok(Response::new(GetReply {
                value: v.to_string(),
            }));
        }
        Ok(Response::new(GetReply::default()))
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<CommonReply>, Status> {
        let req = request.into_inner();
        self.data.remove(&req.key);
        return Ok(Response::new(CommonReply::default()));
    }

    async fn exists(
        &self,
        request: Request<ExistsRequest>,
    ) -> Result<Response<ExistsReply>, Status> {
        let req = request.into_inner();
        Ok(Response::new(ExistsReply {
            flag: self.data.contains_key(&req.key),
        }))
    }
}

pub async fn start_grpc_server_test(stop_sx: broadcast::Sender<bool>) {
    let config = placement_center_conf();
    let server = GrpcServer::new(config.grpc_port);
    server.start_test( stop_sx).await;
}

pub struct GrpcServer {
    port: usize,
}

impl GrpcServer {
    pub fn new(port: usize) -> Self {
        return Self { port };
    }

    pub async fn start_test(&self,stop_sx: broadcast::Sender<bool>){
        let addr = format!("0.0.0.0:{}", self.port).parse().unwrap();
        info!("Broker Grpc Server start. port:{}", self.port);

        let kv_service_handler_test = GrpcKvServicesTest::new();
        let mut stop_rx = stop_sx.subscribe();
        select! {

            val = stop_rx.recv() =>{
                match val{
                    Ok(flag) => {
                        if flag {
                            info!("GRPC Server stopped successfully");

                        }
                    }
                    Err(_) => {}
                }
            },

            val =  Server::builder().add_service(KvServiceServer::new(kv_service_handler_test))
                                    .serve(addr)=>{
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
}