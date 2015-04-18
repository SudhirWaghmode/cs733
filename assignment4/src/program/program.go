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
"math/rand"
)

var mutex = &sync.Mutex{}
var wg sync.WaitGroup			//adding wait group for synchronization between the go routines
var N int = 5					// Number of servers
var Heartbeat_ch = make(chan HeartbeatRPCArgs, 10000)

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

//calls appendRPC to add entry in folowers log
func  AppendCaller() {
	for {
		logentry := <-raft.Append_ch
		raft.C1 <- 1
		var no int
		no = 0;
		for i:=0; i<len(r.clusterConfig.Servers); i++ {			
				if i == r.id { continue }			
				args := &AppendRPCArgs{logentry,r.id}
				var reply string
				rr := make(chan error, 1)
				go func() { rr <- r.clusterConfig.Servers[i].Client.Call("RPC.AppendRPC", args, &reply) } ()
				select{
					case err := <-rr:
						no++;
						if err != nil {	
							log.Println("[Server] AppendRPC Error:", err)
						}
					case <-time.After(1000*time.Millisecond):
						log.Println("AppendRPC time out for: ",i)
						continue //log.Println("Heartbeat reply not got ",i)
				}// inner select loop
			}//end of inner for
			raft.No_Append <- no
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
			
				rr := make(chan error, 1)
				go func() { rr <- r.clusterConfig.Servers[i].Client.Call("RPC.CommitRPC", args, &reply) } ()
				select{
					case err := <-rr://
			//			err := r.clusterConfig.Servers[i].Client.Call("RPC.CommitRPC", args, &reply);
						if err != nil {	
							log.Println("[Server] CommitRPC Error:", err)
						}
					case <-time.After(100*time.Millisecond):
						continue //log.Println("Heartbeat reply not got ",i)
				}// inner select loop			
		
		//	err := r.clusterConfig.Servers[i].Client.Call("RPC.CommitRPC", args, &reply)
		//	if err != nil {
		//	log.Println("[Server] CommitRPC Error:", err)
		//	}
	//	 log.Print("[Server_commit_call] ", reply, " from ", r.clusterConfig.Servers[i].LogPort)
		}//end of inner for
	}//end of outer for
} // end of CommirCaller
//sends heartbeat to followers on regular interval
func SendHeartbeat(){

	for{
	/*	
		randNum := rand.Intn(100) 
		if randNum > 97 && r.id == r.clusterConfig.LeaderId { 
			//r.clusterConfig.Servers[r.id].isLeader=2			//break ThisLoop 
			time.Sleep(100 * time.Second)
			}
	*/
		select{
			
			case <-raft.C1:
				//log.Println("in send SendHeartbeat-Append")
			
			case <-raft.C2:
				//log.Println("in send SendHeartbeat-commit")
			
			case <-time.After(100*time.Millisecond):
				if r.clusterConfig.Servers[r.id].isLeader == 1 {
					for i:=0; i<N; i++ {
							if i == r.id { continue }				
							args := &HeartbeatRPCArgs{r.id,r.currentTerm}				
							var reply string				
							var err error = nil
							rr := make(chan error, 1)
							go func() { rr <- r.clusterConfig.Servers[i].Client.Call("RPC.HeartbeatRPC", args, &reply) } ()
							select{
								case err = <-rr:
									if err != nil {	
										log.Println("[Server] HeartbeatRPC Error:", err) 
									}
								case <-time.After(20*time.Millisecond):
								//	log.Println("Heartbeat reply not got ",i)
									continue //log.Println("Heartbeat reply not got ",i)
							}// inner select loop
					}//end of inner for 
				}//end of if
		}//end of select
	}//end of for loop
}//end of SendHeartbeat()


func  SendVoteRequest(){

	se := r.GetServer(r.id)           //r.clusterConfig.Servers[r.id]
	se.isLeader=0;					  // turns into candidate
									  //0-candidate 2-follower 1-leader
	voteNos:=1
	r.currentTerm++
	r.votedTerm=r.currentTerm
		for i:=0; i<N; i++ {
				
			if i == r.id { continue }					
			args := &VoteInfo{r.currentTerm,se.LsnToCommit}				
			var reply bool = false
			rr := make(chan error, 1)
			go func() { rr <- r.clusterConfig.Servers[i].Client.Call("RPC.VoteForLeader", args, &reply) } ()
					select{
							case err := <-rr:
								if(reply){ voteNos++ }
								if err != nil {	
									log.Println("[Server] VoteForLeader Error:", err) 
								}
							case <-time.After(200*time.Millisecond):
								log.Println("Didn't get Vote reply ",i)
						}// inner select loop
				if voteNos > N/2 { break }		
		
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

	go SendVoteRequest()		
	select{
		case vote := <- raft.C:
				if vote==2 { log.Println(r.id,":Lost Election in Term:",r.votedTerm)
				 } else    {  log.Println(r.id,":Gets Elected in Term:", r.votedTerm) }

		case Lead := <-raft.ElectionTimer_ch:
				r.clusterConfig.Servers[r.id].isLeader=2
				r.clusterConfig.LeaderId=Lead   // set leader			

		case VoteInfo := <-Heartbeat_ch:
			  if VoteInfo.ElectionTerm > r.currentTerm {
					r.clusterConfig.Servers[r.id].isLeader=2
					r.clusterConfig.LeaderId=VoteInfo.LeaderId
				}
	}//end of select

}//end of ElectionCallout

func random(min, max int) int {
    rand.Seed(time.Now().Unix())
    return rand.Intn(max - min) + min
}

func Loop(){
	for {
			select {
				case Lead := <-raft.ElectionTimer_ch:					// for append or commit call
						r.clusterConfig.Servers[r.id].isLeader=2
						r.clusterConfig.LeaderId=Lead
				case VoteInfo := <-Heartbeat_ch:						// for heartbeat 
					  if VoteInfo.ElectionTerm > r.currentTerm {
							r.clusterConfig.Servers[r.id].isLeader=2
							r.clusterConfig.LeaderId=VoteInfo.LeaderId
					}
				case <-time.After(time.Duration(random(2000, 3000))*time.Millisecond):	//for ellection call out
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
/*
func  CommitCaller() {
	for {
		
		logentry := <-raft.Commit_ch
		raft.C2 <- 1
		for i:=0; i<len(r.clusterConfig.Servers); i++ {
		
			if i == r.id { continue }
		 		log.Println(logentry.Lsn())
		 		var reply bool
		 		reply = false
		 		var args *CommitRPCArgs
		 		sz:=len(r.log.Entries)-1
		 		
		 	//for {
		 		
			 		if sz > 0 {
			    	 	args = &CommitRPCArgs{r.log.Entries[sz-1],r.log.Entries[sz],r.id}
			 		}else{
			    	 	args = &CommitRPCArgs{LogEntry_{-1, logentry.Lsn(), logentry.Data(), false},r.log.Entries[sz],r.id}		 			
			 		}
			 		
					err := r.clusterConfig.Servers[i].Client.Call("RPC.CommitRPC", args, &reply)			
					if err != nil {
						log.Println("[Server] CommitRPC Error:", err)
					}
				//	if reply == true {break}
				//	sz = sz-1			
				//}// end of while
				/*
				for j := sz; j<len(r.log.Entries)-1; j++{
					args = &CommitRPCArgs{r.log.Entries[j],r.log.Entries[j+1],r.id}
					err := r.clusterConfig.Servers[i].Client.Call("RPC.CommitRPC", args, &reply)			
					if err != nil {
						log.Println("[Server] CommitRPC Error:", err)
					}
				} //end of j for

				///* //have brace	
		}//end of inner for
	
	}//end of infinite for
} // end of CommirCaller
*/
	// ----------------------
type LogEntry_ struct {
	Term int
	SequenceNumber raft.Lsn
	Command []byte
	IsCommitted bool
}

func (l LogEntry_) Term_() int {
	return l.Term
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

type SharedLog_ struct {
	LsnLogToBeAdded raft.Lsn	
	Entries []LogEntry_
}

func (s *SharedLog_) Init() {
	s.LsnLogToBeAdded = 0
	s.Entries = make([]LogEntry_, 0)
}

// Adds the data into logentry
func (s *SharedLog_) Append(data []byte) (LogEntry_, error) {
	mutex.Lock()
	log := LogEntry_{r.currentTerm, s.LsnLogToBeAdded, data, false}
	s.Entries = append(s.Entries, log)
	s.LsnLogToBeAdded++
	mutex.Unlock()
	return log, nil
}

// Adds the command in leaders shared log to the input_ch
func (s *SharedLog_) Commit(sequenceNumber raft.Lsn, conn net.Conn) {

	se := r.GetServer(r.id)
	lsnToCommit := se.LsnToCommit

	for i:=lsnToCommit; i<=sequenceNumber; i++ {
		if(int(i)<=len(r.log.Entries)){
			raft.Input_ch <- raft.String_Conn{string(r.log.Entries[i].Command), conn}
			r.log.Entries[i].IsCommitted = true
		} else { break }
	}
	se.LsnToCommit++		
}	

// Adds the command in the shared log to the input_ch Through RPC call by leader
func (s *SharedLog_) Commit_follower(Entry_pre LogEntry_, Entry_cur LogEntry_, conn net.Conn) bool {
	se := r.GetServer(r.id)
	i := len(r.log.Entries)
	if i == 1{
		if r.log.Entries[i-1].Term == Entry_cur.Term && r.log.Entries[i-1].SequenceNumber == Entry_cur.SequenceNumber{
			    raft.Input_ch <- raft.String_Conn{string(r.log.Entries[i-1].Command), conn}
				r.log.Entries[i-1].IsCommitted = true
				se.LsnToCommit++
				return true
		}// end of inner if
	} //end i == 1
	
	if i>1{
		if r.log.Entries[i-2].Term == Entry_pre.Term && r.log.Entries[i-2].SequenceNumber == Entry_pre.SequenceNumber{
			if r.log.Entries[i-1].Term == Entry_cur.Term && r.log.Entries[i-1].SequenceNumber == Entry_cur.SequenceNumber{
				raft.Input_ch <- raft.String_Conn{string(r.log.Entries[i-1].Command), conn}
				r.log.Entries[i-1].IsCommitted = true
				se.LsnToCommit++
				return true
			}//end of cur_entry
		}//end of prev_entry
	}//end of index check
	return false
}

type VoteInfo struct {
	ElectionTerm int
	LastCommit raft.Lsn
}
type HeartbeatRPCArgs struct {
	LeaderId int
	ElectionTerm int
}

type AppendRPCArgs struct {
	Entry raft.LogEntry
	LeaderId int
}
/*
type CommitRPCArgs struct {
	Entry_pre LogEntry_
	Entry_cur LogEntry_
	LeaderId int
}
*/
type CommitRPCArgs struct {
	Sequencenumber raft.Lsn
	LeaderId int
}

type RPC struct {
}

//Give vote to valid leader and its own voting term
func (a *RPC) VoteForLeader(args *VoteInfo,reply *bool) error{
	
	se := r.GetServer(r.id)
	if ( (args.ElectionTerm >= r.currentTerm) && (args.LastCommit >= se.LsnToCommit) && (args.ElectionTerm != r.votedTerm) && se.isLeader==2){
		r.votedTerm=args.ElectionTerm
		*reply = true
	} else {
		*reply = false
	}
return nil
}

//Leader send its id through heartbeat 
func (a *RPC) HeartbeatRPC(args *HeartbeatRPCArgs, reply *string) error {

	raft.ElectionTimer_ch <- args.LeaderId
	r.clusterConfig.Servers[args.LeaderId].isLeader=1
	r.clusterConfig.LeaderId=args.LeaderId		
	*reply = "ACK "
	//log.Print(r.id ,"got HeartbeatRPC "," from ",args.LeaderId)
	return nil
}

//Leader appends entery in followers log
func (a *RPC) AppendRPC(args *AppendRPCArgs, reply *string) error {
	
	raft.ElectionTimer_ch <- args.LeaderId
	entry := args.Entry
	r.log.Append(entry.Data())
	*reply = "ACK " +strconv.FormatUint(uint64(entry.Lsn()),10)
	//log.Println(*reply)
	return nil
}

//Leader ask follower to commits entry which is replicated on majority server
/*
func (a *RPC) CommitRPC(args *CommitRPCArgs, reply *bool) error {
	raft.ElectionTimer_ch <- args.LeaderId
	flag := r.log.Commit_follower(args.Entry_pre,args.Entry_cur,nil)
	*reply=flag
	log.Println(*reply)
	return nil
}
*/
func (a *RPC) CommitRPC(args *CommitRPCArgs, reply *string) error {
	raft.ElectionTimer_ch <- args.LeaderId
	r.log.Commit(args.Sequencenumber, nil) // listener: nil - means that it is not supposed to reply back to the client
	*reply = "CACK " +strconv.FormatUint(uint64(args.Sequencenumber),10)
	//	log.Println(*reply)
	return nil
}

// Raft setup
type ServerConfig struct {
	Id int 					// Id of server. Must be unique
	Hostname string 		// name or ip of host
	ClientPort int 			// port at which server listens to client messages.
	LogPort int 			// tcp port for inter-replica protocol messages.
	isLeader int			// 0=candidate 1=leader 2=follower
 	Client *rpc.Client		// Connection object for the server
	LsnToCommit raft.Lsn	// Sequence Number of the last committed log entry

}

type ClusterConfig struct {
	Path string				// Directory for persistent log
	LeaderId int			// ID of the leader
	Servers []ServerConfig	// All servers in this cluster
}

// Raft implements the SharedLog interface.
type Raft struct {
	id int
	log SharedLog_
	clusterConfig *ClusterConfig
	votedTerm int 			//intialise to -1
	currentTerm int 		//intialise to -1
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
	}//end of infinite for loop
	
}

// Accepts an RPC Request
func (r *Raft) AcceptRPC(port int) {

	ap1 := new(RPC)														// Register this function
	rpc.Register(ap1)
	gob.Register(LogEntry_{})											//gob.Register(HeartbeatRPCArgs{})
	listener, e := net.Listen("tcp", "localhost:"+strconv.Itoa(port))
	defer listener.Close()

	if e != nil {
		log.Fatal("[Server] Error in listening:", e)
	}
	for {
		conn, err := listener.Accept();
		if err != nil {
			log.Fatal("[Server] Error in accepting: ", err)
		} else {
			go rpc.ServeConn(conn)
		}
	}//end infinite for
}

// ClientListener is spawned for every client to retrieve the command and send it to the input_ch channel
func (r *Raft) ClientListener(listener net.Conn) {
	
	command, rem := "", ""
	defer listener.Close()
	for {
			input := make([]byte, 1000)
			listener.SetDeadline(time.Now().Add(3 * time.Second)) // 3 second timeout
			listener.Read(input)
			input_ := string(Trim(input))
			
			if len(input_) == 0 { continue }		// If this is not the leader
			
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
						} else {
							commandbytes := []byte(command)
							l, _ := r.log.Append(commandbytes)   			// Append in its own log
							logentry := raft.LogEntry(l)
							raft.Append_ch <- logentry                      //call appendcalller() 
							var append_no int 
							append_no = <-raft.No_Append					//recives no of server on which log relicated
						//	log.Println(r.id," No_Append: ",append_no)
							if append_no > (N+1)/2{ 
								r.log.Commit(r.GetServer(r.id).LsnToCommit, listener) //commit in its own log
 								raft.Commit_ch <- logentry.Lsn()
						    }
						}//ends inner if
					} else { break 	} //end of outer if

					command, rem = GetCommand(rem)
				}//end of for
	}//end of infinite for
}

func PrintKVStore(){

	time.Sleep(time.Second * 32)
//	log.Println(r.id," : ",r.currentTerm,":",r.votedTerm)
/*	if r.id % 2 == 0 {
	//		log.Println("Here it goes size of data : ",len(r.log.Entries))
	for i:=0;i<len(r.log.Entries);i++{
			log.Println(r.id," Data:",string(r.log.Entries[i].Command))
			//	log.Println(raft.KVStore[m])
			}//end of for
	}//end of if*/
}
// Initialize the server
func (r *Raft) Init(config *ClusterConfig, thisServerId int) {
	r.id = thisServerId
	r.clusterConfig = config
	r.log.Init()	
	r.votedTerm=-1;
	r.currentTerm=0;

	go r.AcceptRPC(r.GetServer(r.id).LogPort)	
	count:=0
	for{
		no:=<-raft.Conection_ch
		count = count + no
		if count==N-1 { 
			break 
		}
	}
	go r.AcceptConnection(r.GetServer(r.id).ClientPort)
	go SendHeartbeat()
	go raft.Evaluator()				
	go AppendCaller()
	go CommitCaller()
	go DataWriter()
	go Loop() 					
	go PrintKVStore()

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
			//log.Print("Connected to ", strconv.Itoa(9001+2*i))
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

func main() {

	id, _ := strconv.Atoi(os.Args[1])
	sConfigs := make([]ServerConfig, N)
	for i:=0; i<N; i++ {

		isLeader := 2 		//follwer
		if i != id{ 	
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
	var s string
	fmt.Scanln(&s)
}
