package artnet

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/jsimonetti/go-artnet/packet"
	"github.com/jsimonetti/go-artnet/packet/code"
	"github.com/libp2p/go-reuseport"
)

// NodeCallbackFn gets called when a new packet has been received and needs to be processed
type NodeCallbackFn func(p packet.ArtNetPacket)

// Node is the information known about a node
type Node struct {
	// Config holds the configuration of this node
	Config NodeConfig

	// conn is the UDP connection this node will listen on
	conn          *net.UDPConn
	localAddr     net.UDPAddr
	broadcastAddr net.UDPAddr
	sendCh        chan netPayload
	recvCh        chan netPayload

	// shutdownCh will be closed on shutdown of the node
	shutdownCh   chan struct{}
	shutdown     bool
	shutdownErr  error
	shutdownLock sync.Mutex

	// pollCh will receive ArtPoll packets
	pollCh chan packet.ArtPollPacket
	// pollCh will send ArtPollReply packets
	pollReplyCh chan packet.ArtPollReplyPacket

	log Logger

	callbacks map[code.OpCode]NodeCallbackFn
}

// netPayload contains bytes read from the network and/or an error
type netPayload struct {
	address net.UDPAddr
	err     error
	data    []byte
}

func lastAddr(n *net.IPNet) (net.IP, error) { // works when the n is a prefix, otherwise...
	if n.IP.To4() == nil {
		return net.IP{}, errors.New("does not support IPv6 addresses")
	}
	ip := make(net.IP, len(n.IP.To4()))
	binary.BigEndian.PutUint32(ip, binary.BigEndian.Uint32(n.IP.To4())|^binary.BigEndian.Uint32(net.IP(n.Mask).To4()))
	return ip, nil
}

// NewNode return a Node
func NewNode(name string, style code.StyleCode, interfaceName string, log Logger) (*Node, error) {
	n := &Node{
		Config: NodeConfig{
			Name:      name,
			Type:      style,
			BindIndex: 1,
		},
		conn:     nil,
		shutdown: true,
		log:      log.With(Fields{"type": "Node"}),
	}

	// initialize required node callbacks
	n.callbacks = map[code.OpCode]NodeCallbackFn{
		code.OpPoll:      n.handlePacketPoll,
		code.OpPollReply: n.handlePacketPollReply,
	}

	var ip net.IP
	var broadcast net.IP

	iface, err := net.InterfaceByName(interfaceName)
	if err != nil {
		return nil, err
	}
	addresses, err := iface.Addrs()
	if err != nil {
		return nil, err
	}
	for _, addr := range addresses {
		switch v := addr.(type) {
		case *net.IPNet:
			if v.IP.To4() != nil {
				ip = v.IP
				broadcast, err = lastAddr(v)
				if err != nil {
					return nil, fmt.Errorf("could not find broadcast addr: %e", err)
				}
				break
			}
		}
	}
	if len(ip) == 0 {
		return nil, fmt.Errorf("interface %s has no IP addresses", interfaceName)
	}

	n.Config.IP = ip
	n.broadcastAddr = net.UDPAddr{
		IP:   broadcast,
		Port: int(packet.ArtNetPort),
	}
	n.localAddr = net.UDPAddr{
		IP:   ip,
		Port: packet.ArtNetPort,
		Zone: "",
	}

	return n, nil
}

// Stop will stop all running routines and close the network connection
func (n *Node) Stop() {
	n.shutdownLock.Lock()
	n.shutdown = true
	n.shutdownLock.Unlock()
	close(n.shutdownCh)
	if n.conn != nil {
		if err := n.conn.Close(); err != nil {
			n.log.Printf("failed to close read socket: %v")
		}
	}
}

func (n *Node) isShutdown() bool {
	n.shutdownLock.Lock()
	defer n.shutdownLock.Unlock()
	return n.shutdown
}

// Start will start the controller
func (n *Node) Start() error {
	n.log.With(Fields{"ip": n.Config.IP.String(), "type": n.Config.Type.String()}).Debug("node started")

	n.sendCh = make(chan netPayload, 10)
	n.recvCh = make(chan netPayload, 10)
	n.pollCh = make(chan packet.ArtPollPacket, 10)
	n.pollReplyCh = make(chan packet.ArtPollReplyPacket, 10)
	n.shutdownCh = make(chan struct{})
	n.shutdown = false

	c, err := reuseport.ListenPacket("udp4", fmt.Sprintf(":%d", packet.ArtNetPort))
	if err != nil {
		n.shutdownErr = fmt.Errorf("error net.ListenPacket: %s", err)
		n.log.With(Fields{"error": err}).Error("error net.ListenPacket")
		return err
	}
	n.conn = c.(*net.UDPConn)

	go n.pollReplyLoop()
	go n.recvLoop()
	go n.sendLoop()

	return nil
}

// pollReplyLoop loops to reply to ArtPoll packets
// when a controller asks for continuous updates, we do that using a ticker
func (n *Node) pollReplyLoop() {
	var timer time.Ticker

	binaryPacket := [][]byte{}

	// create an ArtPollReply packet to send out in response to an ArtPoll packet
	packets := ArtPollReplyFromConfig(n.Config)
	for _, packet := range packets {
		me, err := packet.MarshalBinary()
		if err != nil {
			n.log.With(Fields{"err": err}).Error("error creating ArtPollReply packet for self")
			return
		}
		binaryPacket = append(binaryPacket, me)
	}

	// loop until shutdown
	for {
		select {
		case <-timer.C:
			// if we should regularly send replies (can be requested by the controller)
			// we send it here

		case <-n.pollCh:
			// reply with pollReply
			n.log.With(nil).Debug("sending ArtPollReply")

			for _, me := range binaryPacket {
				n.sendCh <- netPayload{
					address: n.broadcastAddr,
					data:    me,
				}
			}

			// TODO: if we are asked to send changes regularly, set the Ticker here

		case <-n.shutdownCh:
			return
		}
	}
}

// sendLoop is used to send packets to the network
func (n *Node) sendLoop() {
	// loop until shutdown
	for {
		select {
		case <-n.shutdownCh:
			return

		case payload := <-n.sendCh:
			if n.isShutdown() {
				return
			}

			num, err := n.conn.WriteToUDP(payload.data, &payload.address)
			if err != nil {
				n.log.With(Fields{"error": err}).Debugf("error writing packet")
				continue
			}
			n.log.With(Fields{"dst": payload.address.String(), "bytes": num}).Debugf("packet sent")

		}
	}
}

// recvLoop is used to receive packets from the network
// it starts a goroutine for dumping the msgs onto a channel,
// the payload from that channel is then fed into a handler
// due to the nature of broadcasting, we see our own sent
// packets to, but we ignore them
func (n *Node) recvLoop() {
	// start a routine that will read data from n.conn
	// and (if not shutdown), send to the recvCh
	go func() {
		b := make([]byte, 4096)
		for {
			num, from, err := n.conn.ReadFromUDP(b)
			if n.isShutdown() {
				return
			}

			if n.localAddr.IP.Equal(from.IP) {
				// this was sent by me, so we ignore it
				//n.log.With(Fields{"src": from.String(), "bytes": num}).Debugf("ignoring received packet from self")
				continue
			}

			if err != nil {
				if err == io.EOF {
					return
				}

				n.log.With(Fields{"src": from.String(), "bytes": num}).Errorf("failed to read from socket: %v", err)
				continue
			}

			n.log.With(Fields{"src": from.String(), "bytes": num}).Debugf("received packet")
			payload := netPayload{
				address: *from,
				err:     err,
				data:    make([]byte, num),
			}
			copy(payload.data, b)
			n.recvCh <- payload
		}
	}()

	// loop until shutdown
	for {
		select {
		case payload := <-n.recvCh:
			p, err := packet.Unmarshal(payload.data)
			if err != nil {
				n.log.With(Fields{
					"src":  payload.address.IP.String(),
					"data": fmt.Sprintf("%v", payload.data),
				}).Warnf("failed to parse packet: %v", err)
				continue
			}

			// at this point we assume that p contains an
			// unmarshalled packet that must have a valid
			// opcode which we can now extract and handle
			// the packet by calling the corresponding
			// callback
			go n.handlePacket(p)

		case <-n.shutdownCh:
			return
		}
	}
}

// handlePacket contains the logic for dealing with incoming packets
func (n *Node) handlePacket(p packet.ArtNetPacket) {
	callback, ok := n.callbacks[p.GetOpCode()]
	if !ok {
		n.log.With(Fields{"packet": p}).Debugf("ignoring unhandled packet")
		return
	}

	callback(p)
}

func (n *Node) handlePacketPoll(p packet.ArtNetPacket) {
	poll, ok := p.(*packet.ArtPollPacket)
	if !ok {
		n.log.With(Fields{"packet": p}).Debugf("unknown packet type")
		return
	}

	n.pollCh <- *poll
}

func (n *Node) handlePacketPollReply(p packet.ArtNetPacket) {
	// only handle these packets if we are a controller
	if n.Config.Type == code.StController {
		pollReply, ok := p.(*packet.ArtPollReplyPacket)
		if !ok {
			n.log.With(Fields{"packet": p}).Debugf("unknown packet type")
			return
		}

		n.pollReplyCh <- *pollReply
	}
}

// RegisterCallback stores the given callback which will be called when a
// packet with the given opcode arrives. This registration function can
// only register callbacks before the node has been started. Calling this
// function multiple times replaces every previous callback.
func (n *Node) RegisterCallback(opcode code.OpCode, callback NodeCallbackFn) {
	if !n.isShutdown() {
		n.log.With(Fields{"opcode": opcode}).Debugf("ignoring callback registration: node has already been started")
		return
	}

	n.callbacks[opcode] = callback
}

// DeregisterCallback deletes a callback stored for the given opcode. This
// deregistration function can only deregister callbacks before the node
// has been started.
func (n *Node) DeregisterCallback(opcode code.OpCode) {
	if !n.isShutdown() {
		n.log.With(Fields{"opcode": opcode}).Debugf("ignoring callback registration: node has already been started")
		return
	}

	delete(n.callbacks, opcode)
}
