package mqttout

import (
	"fmt"
	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/outputs"
	"github.com/elastic/beats/libbeat/outputs/codec"
	"github.com/elastic/beats/libbeat/publisher"
	"github.com/streadway/amqp"
	"strconv"
)

func init() {
	outputs.RegisterType("mqtt", makeMQTTout)
}

type mqttOutput struct {
	beat     beat.Info
	observer outputs.Observer
	codec    codec.Codec
	conn     *amqp.Connection
	client   *amqp.Channel
	q        amqp.Queue
}

func makeMQTTout(
	_ outputs.IndexManager,
	beat beat.Info,
	observer outputs.Observer,
	cfg *common.Config,
) (outputs.Group, error) {

	if !cfg.HasField("index") {
		cfg.SetString("index", -1, beat.Beat)
	}

	config := defaultConfig
	if err := cfg.Unpack(&config); err != nil {
		return outputs.Fail(err)
	}

	// disable bulk support in publisher pipeline
	cfg.SetInt("bulk_max_size", -1, -1)

	mo := &mqttOutput{beat: beat, observer: observer}
	if err := mo.init(beat, config); err != nil {
		return outputs.Fail(err)
	}

	return outputs.Success(-1, 0, mo)

}

func failOnError(err error, msg string) {
	if err != nil {
		logp.Warn("%s: %s", msg, err)
	}
}

func (out *mqttOutput) init(beat beat.Info, config config) error {
	var err error

	enc, err := codec.CreateEncoder(beat, config.Codec)
	if err != nil {
		return err
	}
	out.codec = enc
	link := fmt.Sprintf("amqp://%s:%s@%s:%s/", config.User, config.Password, config.Host, strconv.Itoa(config.Port))
	conn, err := amqp.Dial(link)
	failOnError(err, "Failed to connect to RabbitMQ")
	out.conn = conn
	//defer conn.Close()
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	//defer ch.Close()
	out.client = ch
	//init queue
	q, err := out.client.QueueDeclare(
		config.Topic, // name
		false,        // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare a queue")
	out.q = q
	return nil
}

func (out *mqttOutput) Close() error {
	out.client.Close()
	out.conn.Close()
	return nil
}

func (out *mqttOutput) Publish(batch publisher.Batch) error {
	defer batch.ACK()
	st := out.observer
	events := batch.Events()
	out.observer.NewBatch(len(events))
	dropped := 0
	for i := range events {
		event := &events[i]
		serializedEvent, err := out.codec.Encode(out.beat.Beat, &event.Content)
		if err != nil {
			if event.Guaranteed() {
				logp.Critical("Failed to serialize the event: %v", err)
			} else {
				logp.Warn("Failed to serialize the event: %v", err)
			}
			dropped++
			continue
		}
		body := serializedEvent
		err = out.client.Publish(
			"",         // exchange
			out.q.Name, // routing key
			false,      // mandatory
			false,      // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(body),
			})
		failOnError(err, "Failed to publish a message")
		if err != nil {
			st.WriteError(err)
			if event.Guaranteed() {
				logp.Critical("Publishing event failed with: %v", err)
			} else {
				logp.Warn("Publishing event failed with: %v", err)
			}
			dropped++
			continue
		}
		st.WriteBytes(len(serializedEvent) + 1)
	}
	st.Dropped(dropped)
	st.Acked(len(events) - dropped)
	return nil
}

func (out *mqttOutput) String() string {
	return "MQTT"
}
