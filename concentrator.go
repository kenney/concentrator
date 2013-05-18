package main

import "github.com/kylelemons/go-gypsy/yaml"

import (
 "net"
 "log"
 "os"
 "fmt"
 "bufio"
)

const (
	RECV_BUF_LEN = 1024
)

// Backends are of format server: port.
var Backends map[string]string

/**
 * Main program.
 */
func main() {
	log.Print("Starting up concentrator")

	// Make the backends map.
	Backends = make(map[string]string)

	var file string = "config.yml"
	log.Print("Loading config file: ", file)

	config, err := yaml.ReadFile("config.yml")
	if err != nil {
		log.Fatalf("Error reading config.yml (%q): %s", file, err)
	}

	// Get the backends config list.
	servers, err := yaml.Child(config.Root, "backends")
	server_lst, ok := servers.(yaml.List)
	if !ok {
		log.Fatalf("Could not parse backends list")
 		return
    }

    // Load the stats backends.
	for i := 0; i < server_lst.Len(); i++ {
		node := server_lst.Item(i)
		vals := node.(yaml.Map)
		for index,element := range vals {
			backend_host := fmt.Sprintf("%s", index)
			backend_port := fmt.Sprintf("%s", element)
			Backends[backend_host] = backend_port
			log.Print(fmt.Sprintf("Adding backend %s:%s", backend_host, backend_port))
		}
    }

	port, err := config.GetInt("port")
	host, err := config.Get("host")
	log.Print(fmt.Sprintf("Trying to listen on %s:%v", host, port))

	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%v", host, port))
	if err != nil {
		println("Error starting net.Listen: ", err.Error())
		os.Exit(1)
	}

	log.Print("Server started, awaiting connections...")

	conns := clientConnections(listener)
    for {
        go handleConnections(<-conns)
    }	
}

/**
 * Main program.
 *
 * @return chan net.Conn
 */
func clientConnections(listener net.Listener) chan net.Conn {
    channel := make(chan net.Conn)
    i := 0
    go func() {
        for {
            client, err := listener.Accept()
            if client == nil {
                fmt.Printf("Error on listener.Accept: " + err.Error())
                continue
            }
            i++
            log.Print(fmt.Sprintf("New Client: %d: %v <-> %v\n", i, client.LocalAddr(), client.RemoteAddr()))
            channel <- client
        }
    }()
    return channel
}

/**
 * Process the request from the client.
 *
 * @return void
 */
func handleConnections(client net.Conn) {
    buff := bufio.NewReader(client)
    for {
        line, err := buff.ReadBytes('\n')
        if err != nil {
            break
        }
        client.Write(line)
        retransmitStatsd(string(line[:]))
    }
}

/**
 * Broadcast out the stats message to all backend nodes.
 *
 * @return void
 */
func retransmitStatsd(message string) {
	log.Print("Retransmitting ", message)

	for backend_host,backend_port := range Backends {
		var server = fmt.Sprintf("%s:%s", backend_host, backend_port)
		log.Print("Testing ", server)
		conn, err := net.Dial("udp", server)
		if err != nil {
	   		log.Print("WARNING: Problem with UDP connection: ", err)
	   		continue
		}

		// Send the message to the backend host.
		fmt.Fprintf(conn, message)
	}
}
