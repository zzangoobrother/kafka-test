## KAFKA 적용한 설정 정리

### Producer
- compression.type : 메시지 압축 설정, lz4 설정
- acks : Broker로 부터 메시지 전송 결과를 받는 방법에 대한 설정, 1로 설정
- linger.ms : 배치를 전송하기 전까지 기다리는 최소 시간

### Consumer
- max.partition.fetch.bytes : 파티션당 가져올 최대 메시지 크기
- fetch.min.bytes : 한번 요청에 가져와야할 최소 메시지 크기
- fetch.max.bytes : 한번 요청에 가져와야할 최대 메시지 크기
- Concurrency : consumer의 thread 개수 지정
- BatchListener : n개의 레코드를 동시에 가져오는 방안
- AckMode : 스프링 kafka에서 사용하는 커밋 종류 설정

### Broker
- message.max.bytes : 레코드 배치의 최대 크기
- socket.send.buffer.bytes : 소켓 서버가 사용하는 송수신 버퍼 사이즈
- socket.receive.buffer.bytes : 소켓 서버가 사용하는 송수신 버퍼 사이즈

## 성능 생각해보기
### Throughput : 처리량, 얼마나 많은 데이터를 처리할 수 있는지에 대한 지표
#### Producer
- batch.size : 같은 파티션에 보내는 다수의 레코드를 배치로 묶어 bytes 단위로 전송할 것인지 설정
- linger.ms : 배치 형태의 메시지를 보내기 전에 추가적인 메시지들을 위해 기다리는 시간 조정, producer는 지정된 배치 사이즈에 도달하면 이 옵션과
관계없이 즉시 메시지 전송하고, 배치 사이즈에 도달하지 못한 상황에서 linger.ms 제한 시간에 도달했을 때 메시지 전송
- acks : 해당 옵션은 0, 1, -1(all) 값을 가짐, 값에 따라 producer 메시지를 전달하는 시간을 결정할 수 있음
- buffer.memory
  - producer가 kafka 서버로 데이터를 보내기 위해 잠시 대기할 수 있는 전체 메모리 bytes
  - producer가 보내지 못한 메시지를 보관할 메모리의 크기로 만약 메모리가 full 되면, 다른 메시지 전송를 blocking하게 됨, 또한 메모리 여유가 생기거나, max.block,ms를 초과하면 전송 가능
  - 파티션이 많지 않으면, 조정할 필요가 없지만 파티션이 많다면 메모리를 늘려 blocking 없이 더 많은 데이터가 전송 되도록 설정 필요
- delivery.timeout.ms : send() 호출 후 반환에 대한 성공 또는 실패를 보고하는 시간의 상한, broker로부터 ack를 받기 위해 대기하는 시간이며 재전송에 허용된 시간, retries 옵션 대신 재시도 제한에 대한 설정을 하는 옵션
- enable.idempotence : 메시지 순서를 보장해주는 옵션, batch 0이 실패한다면 뒤에 따라오는 1,2,3,... 후속 batch 들도 실패 처리(OutOfOrderSequenceException)
- max.in.flight.requests.per.connection : 한 번에 몇개의 요청을 전송할 것인지 결정, 1 이면 한번에 하나의 요청 전송하고 응답 받은 후 다음 요청 전송, 2 이상이면 설정된 만큼 요청 전송하고 응답 기다림
- compression.type : 어떤 타입으로 압축해서 보낼지 설정, 압축률이 높으면 한꺼번에 많이 보낼 수 있지만 CPU 사용량이 올라감
  ![readme_image_1.png](image%2Freadme_image_1.png)

### Consumer
- fetch.min.bytes
  - consumer가 한번에 가져올 수 있는 최소 데이터 사이즈 설정, 지정한 사이즈보다 작은 경우 데이터가 누적될 때까지 기다림
  - 해당 옵션을 증가 시키면 broker로 요청한 횟수가 감소하며, broker의 리소스 사용을 절감(producer의 batch.size를 증가 하는 것과 동일 효과)
- fetch.max.wait.ms
  - consumer에서 데이터를 가져오는 최소 시간으로 새로운 데이터가 입력되어도, 해당 시간 이전에는 가져가지 않음
  - consumer가 fetch 요청을 해도, broker가 보내지 않음
- group : consumer group을 활용해서 kafka broker queue에 있는 데이터를 바로 바로 처리하며, 여러 개의 consumer가 처리할 수 있어 처리량이 높아짐

### Broker
- Topic 당 1개의 Partition만 설정 : 순서 보장
- Partition 증가 : 1개 이상의 독립된 스레드가 붙어 작업할 수 있기 때문
- 많은 memory, 많은 file descriptors : 많은 용량의 memory가 있어야 페이지 캐싱 될 메모리 공간이 많아짐(kafka는 페이지 캐싱 적극 활용), file descriptors 개수를 충분히 증가 시켜 producer와 consumer의 connection이 많아질 것을 준비
- message.max.bytes : 한번 요청 batch에서 전송 가능한 최대 bytes, 해당 용량을 넘어가는 요청이 들어오면 전송 실패
- socket.send.buffer.bytes : 소켓 서버가 사용하는 송수신 버퍼 사이즈
- socket.receive.buffer.bytes : 소켓 서버가 사용하는 송수신 버퍼 사이즈

### Latency : 지연시간, 하나의 메시지를 빠르게 전달하는 지표
#### Producer
- linger.ms : 0으로 설정해서 데이터를 수집하는 순간 기다리지 않고 바로 broker로 전송
- compression.type
  - CPU : 압축을 위해 자원 사용
  - network : network bandwidth 사용량 줄어듬
  - 압축률이 적더라도 CPU 사용량이 적은 압축법으로 최대한 지연시간 단축(lz4)
  - 압축을 사용하게 되면 발송에서 약간의 지연이 발생
- acks
  - Broker로 부터 메시지 전송 결과를 받는 방법에 대한 설정
  - 0 : producer는 자신이 보낸 메시지에 대한 ack를 기다리지 않음
  - 1 : Leader Partition으로 부터 데이터 복제 없이 원본만 확인되면 결과를 리턴

#### Consumer
- fetch.min.bytes (default 1) : broker에서 데이터를 가져오는 최소 size, 설정 '1'은 1byte만 있어도 요청 시 바로 전송 (지연 없음)

#### Broker
- Swap memory (비활성화)
  - kafka Heap memory를 초과하면 데이터를 swap 공간으로 복사, kafka의 할당 자원 부족에도 계속 메모리 사용량을 유지할 수 있어 가용성은 올라가지만 성능 저하 발생 (swap 메모리로 빠지면 다시 돌아오게 할 수 없음)
  - vm.swappiness = 0 으로 맞춰 메모리에서만 처리하도록 해야 함.
- Partition 개수 제한 : 많은 Partition 수는 메시지 지연 유발 (partition 복사를 위해 시간을 더 쓰기 때문)
- Broker 수는 많게, Partition 수는 적게 : 하나의 Broker에서 담당하는 복제 본을 줄여 복제에 소요되는 시간을 최소화
- num.relica.fetchers : Follow broker의 I/O 병렬 수준을 정의 (기본 1), leader broker에서 데이터를 복제하는 thread의 개수

### Durability : 내구성, 메시지 유실을 최소화 하는 지에 대한 지표, 중복된 메시지가 없도록 하는 신뢰성 고려 해야함
#### Producer
- acks = all
  - 모든 replica에 복제가 완료된 후 producer에 ack 리턴
  - ack = all 이면, min.insync.replicas = replication.factor 동일하게 설정
- min.insync.replicas
  - ISR 상태를 가지는 replica의 최소 개수
  - acks = all 이라면, producer에 응답하기 위한 replica의 ISR 최소 개수 (복제된 수)
- retries
  - producer 전송 실패 시 자동으로 재전송하는 횟수
  - delivery.timeout.ms를 사용해 재전송 설정을 하는 것을 권장
  - max.in.flight.requests.per.connection = 1로 설정 (한번에 1개 요청), 1개 이상 시 순서가 보장 안됨
- enable.idempotence : 메시지 순서 보장 옵션

#### Consumer
- auto.offset.reset
  - consumer가 비정상적인 종료 혹은 처음 연결 된 이유로 초기 오프셍이 없거나 현재 오프셍이 더 이상 존재하지 않은 경우에 설정 값에 따라 리셋
  - earliest : 가장 초기의 오프셋 값 설정
  - latest : 가장 마지막 오프셋 값으로 설정
  - none : 이전 오프셋 값을 찾지 못하면 에러
  - 메시지 중복 읽기를 최소화 하기 위해 latest 로 설정해야 함
- enable.auto.commit : consumer 오프셋 커밋을 자동으로 할 것인지를 설정하는 매개변수 (default true)
- auto.commit.interval.ms
  - enable.auto.commit 설정 값이 true인 경우 자동으로 오프셋 커밋하는 시간 간격 설정
  - 오프셋 자동 커밋을 자주 할수록 consumer 중단으로 인해 초래될 수 있는 중복 메시지의 수를 줄일 수 있지만 약간의 부하 발생

#### Broker
- replication.factor : 토픽 파티션 별 복재본 수
- default.replication.factor : auto.create.topics.enable가 true인 경우 자동으로 생성되는 topic의 복제 수 설정 (운영 상의 안정성을 위해 auto.create.topics.enable false 권장)
- acks : all, 모든 replica에 복제가 완료된 후, producer에 ack 리턴
- unclean.leader.election.enable : Broker가 죽었을 때, OSR replica도 leader로 선택될 수 있도록 설정 (true), OSR(out-of sync replica) : 죽은 Broker의 최신 메시지를 복사하지 못한 replica -> 데이터 유실 가능
- log.flush.interval.ms
  - 입력된 메시지를 memory 영역의 페이지 캐시에서 disk로 저장하는 수준
  - 값이 클수록 disk I/O가 적게 발생, 메모리 데이터 유실 가능성 높아짐
  - 값이 작을 수록 disk I/O 많이 발생, 메모리 데이터 유실 가능성 거의 없음
  - 이와 관련된 다른 옵션
    - log.flush.interval.messages : 메시지가 디스크 플러시되기 전에 로그 파티션에 누적된 메시지 수
    - log.flush.interval.ms : 토픽의 메시지가 디스크로 플러시 되기 전에 메모리에 보관되는 최대 시간, 설정하지 않으면 log.flush.scheduler.interval.ms 값 사용
    - log.flush.scheduler.interval.ms : 로그 플러시가 로그를 디스크로 플러시해야 하는지 여부를 확인하는 빈도 (ms)

### Availability : 가용성, 서버의 중지 시간을 최소화 하는 지표
#### Broker
- n 슈 유지 : Partition 별 leader 선출에 많은 시간 소요 (복구 시간 증가)
- unvlean.leader.election.enable
  - ISR(in-sync replica)가 아닌 OSR(out-of sync replica)를 가지고 있는 Broker를 leader로 선출 할 수 있도록 설정하는 옵션
  - 가용성을 최대한 보장하기 위한 설정, 데이터의 유실이 발생하더라도 kafka 서버가 중지되는 상황을 막아서 서비스 가용성을 보장
- min.insync.replicas : producer에 응답을 해주기 위한 최소한의 복제 수, 값을 낮게 설정해 최소한의 복제만 된다면 바로 producer가 동작할 수 있도록 가용성을 높일 수 있음
- num.recovery.threads.per.data.dir : log dir들을 기준으로 log data file을 스캔하여 복구에 사용되는 스레드의 최대 수
  - broker의 시작과 종료 시에만 사용되므로 병행 처리를 하도록 많은 수의 thread를 지정하여 장애 브로커를 빠르게 재시동 하는 것이 좋음
