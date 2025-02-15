## KAFKA 적용한 설정 정리

### Producer
- compression.type : 
- acks : 
- linger.ms : 

### Consumer
- max.partition.fetch.bytes : 
- fetch.min.bytes : 
- fetch.max.bytes : 
- Concurrency : 
- BatchListener : 
- AckMode : 

### Broker
- KAFKA_MESSAGE_MAX_BYTES : 
- KAFKA_SOCKET_SEND_BUFFER_BYTES : 
- KAFKA_SOCKET_RECEIVE_BUFFER_BYTES : 

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

