package main

import "github.com/kylelemons/go-gypsy/yaml"
import "github.com/stathat/consistent"
	

import (
 "net"
 "log"
 "os"
 "fmt"
 "bufio"
 "strings"
)

const (
	RECV_BUF_LEN = 1024
)

// Slice of backends.
var Backends []string 

/**
 * Main program.
 */
func main() {
	log.Print("Starting up concentrator")

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
			log.Print(fmt.Sprintf("Adding backend %s:%s", backend_host, backend_port))
			Backends = append(Backends, fmt.Sprintf("%s:%s", backend_host, backend_port))
		}
    }

	for _, backserver := range Backends {
		log.Print(fmt.Sprintf("New server is: %s", backserver))
	}

	port, err := config.GetInt("port")
	host, err := config.Get("host")
	log.Print(fmt.Sprintf("Trying to listen on %s:%v", host, port))

	relay_method, err := config.Get("relay_method")
	log.Print(fmt.Sprintf("We want to relay using %s", relay_method))

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
        	return
        }
        log.Print("Line: ", string(line[:]))
        client.Write(line)
        retransmitUsingConsistentHashing(string(line[:]))
    }
}

/**
 * Broadcast out the stats message to all backend nodes.
 *
 * @return void
 */
func retransmitStatsd(message string) {
	log.Print("Retransmitting ", message)

	for _, server := range Backends {
		log.Print(fmt.Sprintf("Testing server: ", server))
		conn, err := net.Dial("tcp", server)
		if err != nil {
	   		log.Print("WARNING: Problem with TCP connection: ", err)
	   		continue
		}

		// Send the message to the backend host.
		fmt.Fprintf(conn, message)
	}
}

/**
 * Broadcast out the stats message to a single backend.
 *
 * @return void
 */
func retransmitUsingConsistentHashing(message string) {
	log.Print(fmt.Sprintf("Retransmitting %s to the appropriate backend", message))

	cons := consistent.New()

	for _,server := range Backends {
		log.Print(fmt.Sprintf("Adding %s to consistent hash ring", server))
		cons.Add(server)
	}

	// Carbon is format:  $key $value $timestamp
	var message_bits = strings.Fields(message)

	// Get just the $key.
	stat_name := message_bits[0]
	log.Print("Determining backend for message based on ", stat_name)

	// Get which hashed server to use based on the stat name.
	hashed_server, err := cons.Get(stat_name)

	log.Print("Chosen server: ", hashed_server)
	conn, err := net.Dial("tcp", hashed_server)
	if err != nil {
   		log.Print("WARNING: Problem with TCP connection: ", err)
   		return
	}

	// Send the message to the backend host.
	fmt.Fprintf(conn, message)
}
