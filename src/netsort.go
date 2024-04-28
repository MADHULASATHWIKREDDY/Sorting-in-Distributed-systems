package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v2"

	"io"
	"math"
	"net"
	"sort"
	"sync"
)

type Vertex struct {
	key   []byte
	value []byte
}

type ServerConfigs struct {
	Servers []struct {
		ServerId int    `yaml:"serverId"`
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
	} `yaml:"servers"`
}

func readServerConfigs(configPath string) ServerConfigs {
	f, err := os.ReadFile(configPath)

	if err != nil {
		log.Fatalf("could not read config file %s : %v", configPath, err)
	}

	scs := ServerConfigs{}
	err = yaml.Unmarshal(f, &scs)

	return scs
}

// Sending data belonging to one server at once.
func sendData(
	data [][]byte,
	addr string,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		//fmt.Println("Error connecting to server:", err)
		fmt.Println(addr)
		panic(err)
	}
	defer conn.Close()

	// Send data
	for _, line := range data {
		_, err = conn.Write(line)
		/*for {
			_, err = conn.Write(line)
			if err == nil {
				break
			}
		}*/
	}
	fmt.Println("Complete sendData----->")
}

func readAndSendMessages(
	write_only_ch chan<- []byte, // a send only channel is used to send the data packets that belong to this server.
	inputfile string,
	addr string,
	scs ServerConfigs,
	nServers int,
	serverId int,
	wg *sync.WaitGroup,
) {
	/*
	* I will read through this input file and if the given data byte belong to current server,
	* then send it through channel. Else, send it to the destination.
	 */
	// We will read this in chunks
	time.Sleep(5 * time.Second)
	f, err := os.Open(inputfile)
	if err != nil {
		fmt.Println(err)
		fmt.Println("readAndSendMessages")
		os.Exit(1)
	}
	defer f.Close()

	extraByte := byte(0)
	d := make(map[string][][]byte)
	count := 0
	for {
		//Buffer of size 100, we will read 100 Bytes at a time
		buffer := make([]byte, 100)
		_, err := f.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		count++
		buffer = append(buffer, extraByte)
		// Check if the bytes belong to this server or other server
		tserver := int(buffer[0] >> int(8-(math.Log(float64(nServers))/math.Log(2))))

		taddr := scs.Servers[tserver].Host + ":" + scs.Servers[tserver].Port
		d[taddr] = append(d[taddr], buffer)
		//[:n]
		//data= append(data, buffer[:n])
	}
	fmt.Println("Number of lines in Server:", count)
	for i := 0; i < nServers; i++ {
		taddr := scs.Servers[i].Host + ":" + scs.Servers[i].Port
		buffer := make([]byte, 100)
		extraByte = byte(1)
		buffer = append(buffer, extraByte)
		d[taddr] = append(d[taddr], buffer)
	}
	for taddr, values := range d {
		fmt.Println("Toserver:Number of data", taddr, len(values))
		if taddr == addr {
			for _, value := range values {
				write_only_ch <- value
			}
		} else {
			go sendData(values, taddr, wg)
		}
	}
	fmt.Println("Complete readAndSendMessages----->")
}

// --------------------------------------
func listenForClientConnections(
	write_only_ch chan<- []byte, // a channel to which client messages will be sent
	server_address string,
	nServers int,
) {
	// server_address := Host + ":" + Port
	fmt.Println("Starting " + "tcp" + " server on " + server_address)

	clientComp := 0
	listener, err := net.Listen("tcp", server_address)
	if err != nil {
		fmt.Println("Error listening: ", err.Error())
		os.Exit(1)
	}
	defer listener.Close()

	for clientComp < (nServers - 1) {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error connecting: ", err.Error())
			return
		}
		fmt.Println("Client " + conn.RemoteAddr().String() + " connected, spawning goroutine to handle connection")

		// Spawn a new goroutine to handle this client's connections
		// and go back to listening for additional connections
		go handleClientConnection(conn, write_only_ch)
		clientComp++
	}
	fmt.Println("Complete listenForClientConnections----->")
}

func handleClientConnection(
	conn net.Conn, // the connection to handle
	write_only_ch chan<- []byte, // a channel to which incoming messages from this client will be written
) {
	/*
	 * In this simple example, we issue a single conn.Read() call without accounting
	 * for the fact that the client may have more data to send. For the assignment,
	 * please implement code that reads bytes until a record with stream_complete=1
	 * has been received.
	 */
	defer conn.Close()
	for {
		client_msg_buf := make([]byte, 101)
		_, err := conn.Read(client_msg_buf)
		if err == io.EOF {
			break
		}

		if err != nil {
			fmt.Fprintf(os.Stderr, "Conn::Read: err %v\n", err)
			os.Exit(1)
		}
		//message := string(client_msg_buf[0:bytes_read])
		write_only_ch <- client_msg_buf
	}
	fmt.Println("Complete handleClientConnection----->")
}

//--------------------------------------

func receiveDataFromClients(
	read_only_ch <-chan []byte, // a channel from client messages will be read
	nServers int,
	ServerId int,
) []Vertex {
	/*
	 * Here, we wait for exactly 'WorldSize' messages to be received
	 * by performing a blocking read on the channel 'read_only_ch'.
	 *
	 * Note that since this function was called synchronously from main()
	 * i.e. not within a separate goroutine, the main goroutine will wait
	 * till all messages have been received before exiting. Please make
	 * sure that in your implementation, the main goroutine waits for
	 * the completion of all other goroutines it had spawned before
	 * exiting.
	 */
	client_messages := []Vertex{} //[]Vertex{}
	clientComp := 0
	count := 0
	for clientComp < nServers {
		/*
		 * Since we're using an unbuffered channel, the receive operation
		 * will block till some other goroutine writes to this channel
		 */
		message := <-read_only_ch
		count++
		if int(message[len(message)-1]) == 1 {
			clientComp++
		} else {
			tp := Vertex{message[:10], message[10:100]}
			//data= append(data, tp)
			client_messages = append(client_messages, tp)
		}
		fmt.Println("Client comp", clientComp)
		fmt.Println("Number of Messages recieved:", ServerId, count)
	}
	fmt.Println("Complete receiveDataFromClients----->")
	return client_messages
}

func main() {
	// log.LstdFlags--> adds date time where log happened and log.Lshortfile--> location in which and line the log occured.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	// To check whether the input "Terminal" call is according to the specification.
	if len(os.Args) != 5 {
		log.Fatal("Usage : ./netsort {serverId} {inputFilePath} {outputFilePath} {configFilePath}")
	}

	// What is my serverId
	// REading the server ID in command line and converting from str to integer.
	serverId, err := strconv.Atoi(os.Args[1])
	//Errro check if the input string can be converted to an integer.
	if err != nil {
		log.Fatalf("Invalid serverId, must be an int %v", err)
	}
	// Logging the server ID
	fmt.Println("My server Id:", serverId)

	// Read server configs from file and gets the return value as a list of "ServerConfigs" strunctures.
	scs := readServerConfigs(os.Args[4])

	fmt.Println("Got the following server configs:", scs)

	/*
		Implement Distributed Sort
	*/

	// Used to sort the server config. This is done so that Server i will exist at Server[i] and can easily featable.
	sort.Slice(scs.Servers, func(i, j int) bool {
		return scs.Servers[i].ServerId <= scs.Servers[j].ServerId
	})
	// Tested--> Works fine

	// Read the port and address of own server
	addr := scs.Servers[serverId].Host + ":" + scs.Servers[serverId].Port
	nServers := len(scs.Servers)

	inputfile := os.Args[2]
	outputfile := os.Args[3]

	var wg sync.WaitGroup

	bidirectional_ch := make(chan []byte)
	wg.Add(nServers - 1)
	go listenForClientConnections(bidirectional_ch, addr, nServers)
	go readAndSendMessages(bidirectional_ch, inputfile, addr, scs, nServers, serverId, &wg)
	//nServers:=1// Change it to the config file input
	client_messages := receiveDataFromClients(bidirectional_ch, nServers, serverId)
	wg.Wait() // Check here

	// Sorting the records belonging to this server.
	sort.Slice(client_messages, func(i, j int) bool {
		// Sort the two records by the key
		return bytes.Compare(client_messages[i].key[:], client_messages[j].key[:]) == -1
	})

	// Create the Output file
	file, err := os.Create(outputfile)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	for _, line := range client_messages {
		file.Write(line.key[:])
		file.Write(line.value[:])
		//file.Write(line)
	}
}
