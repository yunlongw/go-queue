package rabbitmq

import (
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/yunlongw/go-queue/queue"
)

type (
	// ConsumeHandle func(message string) error

	ConsumeHandler interface {
		Consume(message string) error
	}

	RabbitListener struct {
		conn    *amqp.Connection
		channel *amqp.Channel
		forever chan bool
		handler map[string]ConsumeHandler
		queues  RabbitListenerConf
	}
)

func MustNewListener(listenerConf RabbitListenerConf, handler map[string]ConsumeHandler) queue.MessageQueue {
	listener := &RabbitListener{queues: listenerConf, handler: handler, forever: make(chan bool)}
	conn, err := amqp.Dial(getRabbitURL(listenerConf.RabbitConf))
	if err != nil {
		log.Fatalf("failed to connect rabbitmq, error: %v", err)
	}

	listener.conn = conn
	channel, err := listener.conn.Channel()
	if err != nil {
		log.Fatalf("failed to open a channel: %v", err)
	}

	listener.channel = channel
	return listener
}

func (q RabbitListener) Start() {
	for _, que := range q.queues.ListenerQueues {
		err := q.consume(que)
		if err != nil {
			log.Println(err)
		}
	}

	<-q.forever
}

func (q RabbitListener) consume(que ConsumerConf) error {
	msgChan := make(chan amqp.Delivery, 100) // 创建带缓冲的通道

	msg, err := q.channel.Consume(
		que.Name,
		"",
		que.AutoAck,
		que.Exclusive,
		que.NoLocal,
		que.NoWait,
		nil,
	)
	if err != nil {
		log.Fatalf("failed to listener, error: %v", err)
		return err
	}

	go func() {
		for d := range msgChan {
			if handle, ok := q.handler[que.Name]; ok == true {
				if err := handle.Consume(string(d.Body)); err != nil {
					log.Println(fmt.Sprintf("Error on consuming: %s, error: %v", string(d.Body), err))
				}
			} else {
				log.Println("消费者不存在，请检查配置")
			}

			d.Ack(false) // 手动确认消息处理完成
		}
	}()

	for d := range msg {
		msgChan <- d // 将消息发送到带缓冲的通道，非阻塞
	}

	return nil
}

func (q RabbitListener) Stop() {
	q.channel.Close()
	q.conn.Close()
	close(q.forever)
}
