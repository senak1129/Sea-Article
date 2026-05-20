创建微服务需要使用以下命令，运行命令的目录为主目录
```goctl
goctl rpc protoc proto/user.proto --go_out=./service/user/rpc/pb --go-grpc_out=./service/user/rpc/pb --zrpc_out=./service/user/rpc --style=go_zero
```
以下为示例的生成的文件夹目录
```
└─service
    └─user
        └─rpc
            │  user.go
            │  
            ├─etc
            │      user.yaml
            │      
            ├─internal
            │  ├─config
            │  │      config.go
            │  │      
            │  ├─logic
            │  │      create_user_logic.go
            │  │      delete_user_logic.go
            │  │      get_user_logic.go
            │  │      login_logic.go
            │  │      logout_logic.go
            │  │      update_user_logic.go
            │  │      
            │  ├─server
            │  │      user_service_server.go
            │  │      
            │  └─svc
            │          service_context.go
            │          
            ├─pb
            │      user.pb.go
            │      user_grpc.pb.go
            │      
            └─userservice
                    user_service.go
```
其中etc目录下会生成一个yaml文件是需要手动配置的，其他都是不需要手动配置的，示例配置的内容如下，需要根据自身微服务进行更改:
```yaml
Name: user.rpc
ListenOn: 0.0.0.0:8080
Etcd:
  Hosts:
  - 127.0.0.1:2379
  Key: user.rpc

Postgres:
  Host: 127.0.0.1
  Port: "5432"
  User: root
  Password: "123456"
  DBName: test

Log:
  Mode: file                 
  Path: ../../../log/user
  Level: info               
  Compress: true             
  KeepDays: 7                
  StackCooldownMillis: 100   
```