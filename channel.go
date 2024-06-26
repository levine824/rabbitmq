package rabbitmq

import (
	"github.com/streadway/amqp"
)

// Channel wraps an AMQP channel with listeners.
type Channel struct {
	*amqp.Channel

	connection *Connection

	id int

	closes   chan *amqp.Error
	returns  chan amqp.Return
	confirms chan amqp.Confirmation
}

// Constructs a new channel. Before using, channel should be opened.
func newChannel(c *Connection, id int) *Channel {
	return &Channel{
		connection: c,
		id:         id,
		closes:     make(chan *amqp.Error, 1),
		returns:    make(chan amqp.Return, 1),
		confirms:   make(chan amqp.Confirmation, 1),
	}
}

func (ch *Channel) open() error {
	c, err := ch.connection.amqConnection.Channel()
	if err != nil {
		return err
	}

	ch.Channel = c

	return nil
}

// Close closes the channel logically.
func (ch *Channel) Close() {
	ch.connection.closeChannel(ch)
}

func (ch *Channel) close() error {
	err := ch.Channel.Close()
	if err != nil {
		return err
	}

	ch.Channel = nil
	ch.closes = nil
	ch.returns = nil
	ch.confirms = nil

	return nil
}

func (ch *Channel) NotifyClose() chan *amqp.Error {
	if ch.closes == nil {
		ch.closes = ch.Channel.NotifyClose(make(chan *amqp.Error, 1))
	}

	return ch.closes
}

func (ch *Channel) NotifyReturn() chan amqp.Return {
	if ch.returns == nil {
		ch.returns = ch.Channel.NotifyReturn(make(chan amqp.Return, 1))
	}

	return ch.returns
}

func (ch *Channel) NotifyPublish() (chan amqp.Confirmation, error) {
	if ch.confirms == nil {
		if err := ch.Channel.Confirm(false); err != nil {
			return nil, err
		}
		ch.confirms = ch.Channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	}

	return ch.confirms, nil
}
