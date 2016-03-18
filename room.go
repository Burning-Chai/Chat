package main

import (
    "github.com/gorilla/websocket"
    "net/http"
    "log"
)

const (
    socketBufferSize = 1024
    messageBufferSize = 256
)

type room struct {
    forward chan []byte
    join    chan *client
    leave   chan *client
    clients map[*client]bool
}

func (r *room) run() {
    for {
        select {
        case client := <-r.join:
            r.clients[client] = true
        case client := <-r.leave:
            delete(r.clients, client)
            close(client.send)
        case msg := <-r.forward:
            for client := range r.clients {
                select {
                case client.send <- msg:
                default:
                    delete(r.clients, client)
                    close(client.send)
                }
            }
        }
    }
}

var upgrader = &websocket.Upgrader{ReadBufferSize: socketBufferSize, WriteBufferSize: messageBufferSize}

func (r *room) ServeHTTP(w http.ResponseWriter, req *http.Request) {
    socket, err := upgrader.Upgrade(w, req, nil)
    if err != nil {
        log.Fatal("ServerHTTP:", err)
        return
    }
    client := &client{
        socket: socket,
        send: make(chan []byte, messageBufferSize),
        room: r,
    }
    r.join <- client
    defer func() {
        r.leave <- client
    }()
    go client.write()

    client.read()
}
