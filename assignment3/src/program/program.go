package main
import (
"fmt"
"raft"
"log"
"time"
"encoding/gob"
"strings"
"net"
"net/rpc"
"os"
"strconv"
"sync"
)

//adding wait group for synchronization between the go routines
var wg sync.WaitGroup

var N int = 3	// Number of servers

// Reads send-requests from the output channel and processes it
func  DataWriter() {
	for {
		replych := <-raft.Output_ch
		text := replych.Text
		conn := replych.Conn	// nil for followers as they don't have to send the reply back
		if text != "" && conn != nil {
			conn.Write([]byte(text))
			}
		}
}

//
func  AppendCaller() {
	for {
		logentry := <-raft.Append_ch
		raft.C1 <- 1
		for i:=0; i<len(r.clusterConfig.Servers); i++ {
			
				if i == r.id { continue }
			
				args := &AppendRPCArgs{logentry,r.id}
			
				var reply string
			
				err := r.clusterConfig.Servers[i].Client.Call("RPC.AppendRPC", args, &reply)

				if err != nil {
					log.Println("[Server] AppendRPC Error:", err)
				}
			
				log.Print("[Server_append_call] ", reply, " from ", r.clusterConfig.Servers[i].LogPort)
			
			}//end of inner for
		}//end of outer for
}

func  CommitCaller() {
	for {
		lsn := <-raft.Commit_ch
		raft.C2 <- 1
		// Commit it to everyone's KV Store
		for i:=0; i<len(r.clusterConfig.Servers); i++ {
			if i == r.id { continue }
			args := &CommitRPCArgs{lsn,r.id}
			var reply string
			err := r.clusterConfig.Servers[i].Client.Call("RPC.CommitRPC", args, &reply)
			if err != nil {
			log.Println("[Server] CommitRPC Error:", err)
			}
		 log.Print("[Server_commit_call] ", reply, " from ", r.clusterConfig.Servers[i].LogPort)
		}
	}
}


func SendHeartbeat(){

	for{
		select{
			case <-raft.C1:
				log.Println("in send SendHeartbeat-Append")
			case <-raft.C2:
				log.Println("in send SendHeartbeat-commit")
			case <-time.After(100*time.Millisecond):
				if r.id == r.clusterConfig.LeaderId {
					for i:=0; i<N; i++ {
							if i == r.id { continue }				
							args := &HeartbeatRPCArgs{r.id}				
							var reply string				
							err := r.clusterConfig.Servers[i].Client.Call("RPC.HeartbeatRPC", args, &reply)
							log.Println("Heartbeat sent to ",i)
							if err != nil {	
								log.Println("[Server] HeartbeatRPC Error:", err) 
							}							
					}//end of inner for 
				}//end of if
		}//end of select
	}//end of for loop

}//end of SendHeartbeat()


func  SendVoteRequest(){
	se := r.GetServer(r.id)           //r.clusterConfig.Servers[r.id]
	se.isLeader=0;					  // turns into candidate
	voteNos:=1
	r.currentTerm++
	for i:=0; i<N; i++ {
				
					if i == r.id { continue }					
					args := &VoteInfo{r.currentTerm,se.LsnToCommit}				
					var reply bool = false
					err := r.clusterConfig.Servers[i].Client.Call("RPC.VoteForLeader", args, &reply)
					if err != nil {	continue }
					if(reply){ voteNos++ }

				}//end of inner for
			
			if(voteNos > N/2){
				r.clusterConfig.Servers[r.id].isLeader=1
				r.clusterConfig.LeaderId=r.id	
			raft.C <- 1
			} else {
				r.clusterConfig.Servers[r.id].isLeader=2  //falling to follower state
			raft.C <- 2
			}
				
}// end  of func SendVoteRequest()

func  ElectionCallout(){
	
//	log.Print("Election Call out \n")
	go SendVoteRequest()		
	select{
		case vote := <- raft.C:
				if vote==2 {     //***********always check vote value
				//	SendHeartbeat() 
				log.Print("vote=2 Not elected \n")
				} else {
				log.Print("vote=1  elected \n", r.id)
				}

		case Lead := <-raft.ElectionTimer_ch:
				r.clusterConfig.Servers[r.id].isLeader=2
				r.clusterConfig.LeaderId=Lead   // set leader			

	//	case <-time.After(200*time.Millisecond):
	//		return nil
	}//end of select

}//end of ElectionCallout

func Loop(){
	for {
			 //have to select from range
			select {
				case Lead := <-raft.ElectionTimer_ch:
						r.clusterConfig.Servers[r.id].isLeader=2
						r.clusterConfig.LeaderId=Lead
				case <-time.After(2000*time.Millisecond):
						if r.id == r.clusterConfig.LeaderId { 
						}else{
						ElectionCallout()							
					}
			}//end of select	
		
		}//end of for	
}//end of RequestVoteRPC funnction


// Retrieves the next command out of the input buffer
func GetCommand(input string) (string, string) {
		inputs := strings.Split(input, "\r\n")
		n1 := len(inputs)
		n := len(inputs[0])
		// abc := input[0:3]
		// log.Printf("**%s--%s--%s--%s-", input, inputs[0], (inputs[0])[1:3], abc)
		com, rem := "", ""
		if n >= 3 && (inputs[0][0:3] == "set" || inputs[0][0:3] == "cas") {
			// start of a 2 line command
			if n1 < 3 { // includes "\r\n"
				return "", input // if the command is not complete, wait for the rest of the command
			}
			var in = strings.Index(input, "\r\n") + 2
			in += strings.Index(input[in:], "\r\n") + 2
			com = input[:in]
			rem = input[in:]
		} else if (n >= 3 && inputs[0][0:3] == "get") || (n >= 4 && inputs[0][0:4] == "getm") ||(n >= 6 && inputs[0][0:6] == "delete") {
			// start of a 1 line command
			if n1 < 2 { // includes "\r\n"
				return "", input // if the command is not complete, wait for the rest of the command
			}
			var in = strings.Index(input, "\r\n") + 2
			com = input[:in]
			rem = input[in:]
		} else {
			return "", input
		}
		return com, rem
}

func Trim(input []byte) []byte {
	i := 0
	for ; input[i] != 0; i++ {}
	return input[0:i]
}//end of trim function

var r *Raft // Contains the server details
	// ----------------------
type LogEntry_ struct {
	Term int
	SequenceNumber raft.Lsn
	Command []byte
	IsCommitted bool
}

func (l LogEntry_) Lsn() raft.Lsn {
	return l.SequenceNumber
}

func (l LogEntry_) Data() []byte {
	return l.Command
}
func (l LogEntry_) Committed() bool {
	return l.IsCommitted
}
// ----------------------------------------------
type SharedLog_ struct {
	LsnLogToBeAdded raft.Lsn	// Sequence number of the log to be added
	Entries []LogEntry_
}

func (s *SharedLog_) Init() {
	s.LsnLogToBeAdded = 0
	s.Entries = make([]LogEntry_, 0)
}

// Adds the data into logentry
func (s *SharedLog_) Append(data []byte) (LogEntry_, error) {
	log := LogEntry_{r.currentTerm, s.LsnLogToBeAdded, data, false}
	s.Entries = append(s.Entries, log)
	s.LsnLogToBeAdded++
	return log, nil
}

// Adds the command in the shared log to the input_ch
func (s *SharedLog_) Commit(sequenceNumber raft.Lsn, conn net.Conn) {
	se := r.GetServer(r.id)
	lsnToCommit := se.LsnToCommit
	log.Println(lsnToCommit)
	// Adds the commands to the input_ch to be furthur processed by Evaluator
	for i:=lsnToCommit; i<=sequenceNumber; i++ {
		if(int(i)<len(r.log.Entries)){
			raft.Input_ch <- raft.String_Conn{string(r.log.Entries[i].Command), conn}
			r.log.Entries[i].IsCommitted = true
		} else { break }
	}
	se.LsnToCommit++
}


type VoteInfo struct {
	ElectionTerm int
	LastCommit raft.Lsn
}
type HeartbeatRPCArgs struct {
	LeaderId int
}

type AppendRPCArgs struct {
	Entry raft.LogEntry
	LeaderId int
}
type CommitRPCArgs struct {
	Sequencenumber raft.Lsn
	LeaderId int
}


type RPC struct {
}

func (a *RPC) VoteForLeader(args *VoteInfo,reply *bool) error{
   se := r.GetServer(r.id)
	if ( (args.ElectionTerm >= r.currentTerm) && (args.LastCommit >= se.LsnToCommit) && (args.ElectionTerm != r.votedTerm) && se.isLeader==2){
		r.votedTerm++
		*reply = true
	} else {
		*reply = false
	}
return nil
}

func (a *RPC) HeartbeatRPC(args *HeartbeatRPCArgs, reply *string) error {
//
	//log.Println("YO")
	raft.ElectionTimer_ch <- args.LeaderId

	r.clusterConfig.Servers[args.LeaderId].isLeader=1
	r.clusterConfig.LeaderId=args.LeaderId		
	*reply = "ACK "
	log.Print("HeartbeatRPC got ",r.id ," from ",args.LeaderId)
//	log.Println(" to", args.LeaderId)
//	raft.ElectionTimer_ch <- args.LeaderId
	return nil
}

func (a *RPC) AppendRPC(args *AppendRPCArgs, reply *string) error {
	raft.ElectionTimer_ch <- args.LeaderId

	entry := args.Entry
	r.log.Append(entry.Data())
	*reply = "ACK " +strconv.FormatUint(uint64(entry.Lsn()),10)
			log.Println(*reply)
	return nil
}

func (a *RPC) CommitRPC(args *CommitRPCArgs, reply *string) error {
	raft.ElectionTimer_ch <- args.LeaderId
	r.log.Commit(args.Sequencenumber, nil) // listener: nil - means that it is not supposed to reply back to the client
	*reply = "CACK " +strconv.FormatUint(uint64(args.Sequencenumber),10)
		log.Println(*reply)
	return nil
}
// ----------------------------------------------
// --------------------------------------
// Raft setup
type ServerConfig struct {
	Id int // Id of server. Must be unique
	Hostname string // name or ip of host
	ClientPort int // port at which server listens to client messages.
	LogPort int // tcp port for inter-replica protocol messages.
	isLeader int	// 0=candidate 1=leader 2=follower
 	Client *rpc.Client	// Connection object for the server
	LsnToCommit raft.Lsn	// Sequence Number of the last committed log entry

}

type ClusterConfig struct {
	Path string	// Directory for persistent log
	LeaderId int	// ID of the leader
	Servers []ServerConfig	// All servers in this cluster
}
// ----------------------------------------------
// Raft implements the SharedLog interface.
type Raft struct {
	id int
	log SharedLog_
	clusterConfig *ClusterConfig
	votedTerm int 			//intialise to -1
	currentTerm int 			//intialise to -1
}

func (r Raft) GetServer(id int) *ServerConfig {
	return &r.clusterConfig.Servers[id]
}


// Accepts an incoming connection request from the Client, and spawn a ClientListener
func (r *Raft) AcceptConnection(port int) {
	tcpAddr, error := net.ResolveTCPAddr("tcp", "localhost:"+strconv.Itoa(port))
	if error != nil {
		log.Print("[Server] Can not resolve address: ", error)
	}
	ln, err := net.Listen(tcpAddr.Network(), tcpAddr.String())
	if err != nil {
		log.Print("[Server] Error in listening: ", err)
	}
	defer ln.Close()
	for {
		// New connection created
		listener, err := ln.Accept()
		if err != nil {
		log.Print("[Server] Error in accepting connection: ", err)
		return
		}
		// Spawn a new listener for this connection
		go r.ClientListener(listener)
	}
	
}

// Accepts an RPC Request
func (r *Raft) AcceptRPC(port int) {
// Register this function
	ap1 := new(RPC)
	rpc.Register(ap1)
	gob.Register(LogEntry_{})
	//gob.Register(HeartbeatRPCArgs{})
	
	listener, e := net.Listen("tcp", "localhost:"+strconv.Itoa(port))
//			log.Println("error 1")
	
	defer listener.Close()
   log.Println("came to AppendRPC")	
	if e != nil {
		log.Fatal("[Server] Error in listening:", e)
	}
	for {
//		log.Print("error 2 before")
		conn, err := listener.Accept();
//			log.Println("error 2")

		if err != nil {
			log.Fatal("[Server] Error in accepting: ", err)
		} else {
//			log.Println("here it lies 1")
			go rpc.ServeConn(conn)
//			log.Println("here it lies problem")
		}

	}//end of for
//			log.Println("here its end")
}

// ClientListener is spawned for every client to retrieve the command and send it to the input_ch channel
func (r *Raft) ClientListener(listener net.Conn) {
	command, rem := "", ""
	defer listener.Close()
	// log.Print("[Server] Listening on ", listener.LocalAddr(), " from ", listener.RemoteAddr())
	for {			//this is one is outer for
	// Read
	input := make([]byte, 1000)
	listener.SetDeadline(time.Now().Add(3 * time.Second)) // 3 second timeout
//	log.Println("time out happening")
	listener.Read(input)
	input_ := string(Trim(input))
	
	if len(input_) == 0 { continue }
	// If this is not the leader
	if r.id != r.clusterConfig.LeaderId {
		leader := r.GetServer(r.clusterConfig.LeaderId)
		raft.Output_ch <- raft.String_Conn{"ERR_REDIRECT " + leader.Hostname + " " + strconv.Itoa(leader.ClientPort), listener}
		input = input[:0]
		continue
	}
	command, rem = GetCommand(rem + input_)
	// For multiple commands in the byte stream

	for {
			if command != "" {
				if command[0:3] == "get" {
					raft.Input_ch <- raft.String_Conn{command, listener}
					//log.Print(command)
				} else {
					// log.Print("Command:",command)
					commandbytes := []byte(command)
					// Append in its own log
					l, _ := r.log.Append(commandbytes)
					// Add to the channel to ask everyone to append the entry
					logentry := raft.LogEntry(l)
					raft.Append_ch <- logentry
		//log.Println("send to Append_ch")
					// ** After getting majority votes
					// Commit in its own store
					// log.Print(r.GetServer(r.id).LsnToCommit)
					r.log.Commit(r.GetServer(r.id).LsnToCommit, listener)
					// Add to the channel to ask everyone to commit
					raft.Commit_ch <- logentry.Lsn()
		//log.Println("send to Commit_ch")

				}//ends inner if
			} else { break 	} //end of outer if

			command, rem = GetCommand(rem)
		}//end of for
	}//end of outer for
}
// Initialize the server
func (r *Raft) Init(config *ClusterConfig, thisServerId int) {
	r.id = thisServerId
	r.clusterConfig = config
	r.log.Init()	
	r.votedTerm=-1;
	r.currentTerm=0;
	
	go r.AcceptConnection(r.GetServer(r.id).ClientPort)

	go r.AcceptRPC(r.GetServer(r.id).LogPort)	
	count:=0
	for{
//		log.Print("efire")
		no:=<-raft.Conection_ch
//		log.Print("efiress") 
		count = count + no
		if count==N-1 { 
//		log.Println("I m here")
			break 
		}
	}

	go SendHeartbeat()
	go raft.Evaluator()	//what about object of this one
	go AppendCaller()
	go CommitCaller()
	go DataWriter()
	go Loop() //if he is not leader

}

// ----------------------------------------------
// Creates a raft object. This implements the SharedLog interface.
// commitCh is the channel that the kvstore waits on for committed messages.
// When the process starts, the local disk log is read and all committed
// entries are recovered and replayed
func NewRaft(config *ClusterConfig, thisServerId int, commitCh chan raft.LogEntry) (*Raft, error) {
	raft := new(Raft)
	raft.Init(config, thisServerId)
	return raft, nil
}

func ConnectToServers(i int, isLeader int,Servers []ServerConfig) {
	// Check for connection unless the ith server accepts the rpc connection
	var client *rpc.Client = nil
	var err error = nil
	// Polls until the connection is made
	for {
		client, err = rpc.Dial("tcp", "localhost:" + strconv.Itoa(9001+2*i))
		if err == nil {
			log.Print("Connected to ", strconv.Itoa(9001+2*i))
			raft.Conection_ch <- 1
			break
			}
		}
		
		Servers[i] = ServerConfig{
			Id: i,
			Hostname: "Server"+strconv.Itoa(i),
			ClientPort: 9000+2*i,
			LogPort: 9001+2*i,
			isLeader: isLeader,
			Client: client,
			LsnToCommit: 0,
		}
		
}

// --------------------------------------------------------------
func main() {
	id, _ := strconv.Atoi(os.Args[1])
	sConfigs := make([]ServerConfig, N)
	for i:=0; i<N; i++ {
		isLeader := 2 //follwer
		
//		if i == 0 { isLeader = 1  }
		
		if i != id{ // If it is the leader connecting to every other server....?
			go ConnectToServers(i, isLeader,sConfigs)
		} else {
			// Its own detail
			sConfigs[i] = ServerConfig{
				Id: i,
				Hostname: "Server"+strconv.Itoa(i),
				ClientPort: 9000+2*i,
				LogPort: 9001+2*i,
				isLeader: isLeader,
				Client: nil,
				LsnToCommit: 0,
				
			}
		}
	}//end of for loop


	cConfig := ClusterConfig{"undefined path", -1, sConfigs}
	commitCh := make(chan raft.LogEntry,100)
	wg.Add(1)
	r, _ = NewRaft(&cConfig, id, commitCh)
	wg.Wait()
	// Wait until some key is press
	var s string
	fmt.Scanln(&s)
}
