package main

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
