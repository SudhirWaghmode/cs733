package assignment1
import (
	"net"
	"os"
	"sync"
	"strings"
	"fmt"
	"strconv"
)

type Node struct {
	expt int64
	numByte int64
	value string
	version int64
//	timeStamp int64

}

var m map[string]Node
var verSion int64=0
func main() {	
}
func server() {
	
	m = make(map[string]Node)
	service := ":9011"
	

	tcpAddr, err := net.ResolveTCPAddr("ip4", service)
	checkError(err)

	listener, err := net.ListenTCP("tcp", tcpAddr)
	checkError(err)

	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
	 	go handleClient(conn)
	}
	
}
func handleClient(conn net.Conn) {
	
	defer conn.Close()
	var mutex = &sync.Mutex{}
	buf:=make([]byte,1024)

	for {		
		n, err := conn.Read(buf[0:])
		if err != nil {
			return
		}
		
		s := strings.Split(string(buf[0:n])," ")

		switch string(s[0]) {
		case "set":
			 mutex.Lock()	
			 set_body(s)
			 mutex.Unlock()	
				_, err2 := conn.Write([]byte("OK\r\n"))
				if err2 != nil {
					return
				}
			
		case "get":
			_,ok:=m[s[1]]
			if ok{
			_, err2 := conn.Write([]byte("VALUE "+strconv.FormatInt(m[s[1]].numByte,10)+" \r\n"+m[s[1]].value+"\r\n"))
				if err2 != nil {
					return
				}
			}else{
				_,err2:= conn.Write([]byte("Error"))
				if err2 != nil {
					return
				}
			}
		case "getm":
			_,ok:=m[s[1]]
			if ok{
			_, err2 := conn.Write([]byte("VALUE "+strconv.FormatInt(m[s[1]].version,10)+" "+strconv.FormatInt(m[s[1]].expt,10)+" "+strconv.FormatInt(m[s[1]].numByte,10)+"\r\n"+m[s[1]].value+"\r\n"))
			if err2 != nil {
					return
				}
			}
		case "delete":
				_,ok:=m[s[1]]
				mutex.Lock()	
				delete(m,string(s[1]))
				mutex.Unlock()	
				if ok {
					_, err2 := conn.Write([]byte("DELETED\r\n"))
					if err2 != nil {
						return
					} 
				}else{
					_, err2 := conn.Write([]byte("Error1"))
					if err2 != nil {
						return
				}
			}
		}		
	}
}


func set_body(s []string) {
		k := strings.Split(string(s[3]),"\r\n")
		int1,err :=(strconv.Atoi(s[2]))
		if err != nil{
			fmt.Println("sa")	
		} 
		int2,err :=(strconv.Atoi(k[0]))
		if err != nil{
		fmt.Println("sa")	
		} 
		_,ok:=m[s[1]]
		
		if !ok {
			m[s[1]]=Node{int64(int1),int64(int2),k[1],int64(1)}
		}else{
			val:=m[s[1]].version
			m[s[1]]=Node{int64(int1),int64(int2),k[1],val+1}
		}
		
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}
